from .codec import digest, dumps, loads, name
from .errors import Conflict, InvalidDocument, NotFound, SnapshotTooOld
from .mvcc import checked_revision

MAX_COLLECTIONS = 256
UNSET = object()


class FeedPage(list):
    """A list of delivered commit views plus the facts needed to resume.

    Behaving like a plain list keeps ``read`` compatible with callers that only
    iterate or compare commits.  The extra attributes explain what the page
    costs to consume:

    ``next``
        Highest revision examined by this page.  Acknowledging it is always
        safe: every commit up to it has been either delivered or reported in
        ``skipped``, and no later commit was passed over.
    ``skipped``
        Revisions whose operations were all outside the selection.  They are
        still accounted for, so a filtered tail can advance the consumer
        instead of holding the retained floor forever.
    ``head`` / ``floor``
        Watermarks observed while the page was read.
    ``selection`` / ``complete``
        The normalized watched set, or ``None`` when whole commits are read.
    """

    def __init__(self, events, *, skipped, next, examined, head, floor, selection, complete):
        super().__init__(events)
        self.skipped = skipped
        self.next = next
        self.examined = examined
        self.head = head
        self.floor = floor
        self.selection = selection
        self.complete = complete
        self.consumer = None
        self.ack = None

    @property
    def remaining(self):
        """Revisions after ``next`` that this page has not examined yet."""
        return max(0, self.head - self.next)

    def to_dict(self):
        """Structured view of the page for logging or JSON output."""
        return {"events": list(self), "next": self.next, "skipped": list(self.skipped),
                "examined": self.examined, "remaining": self.remaining,
                "head": self.head, "floor": self.floor,
                "selection": self.selection, "complete": self.complete}

    def __repr__(self):
        return "FeedPage(%s, next=%r, skipped=%d)" % (list.__repr__(self), self.next, len(self.skipped))


def _selection(collections):
    """Normalize a watched-collection set.

    ``None`` keeps the original meaning of the feed: every complete commit is
    delivered.  A non-empty iterable of collection names narrows delivery to
    commits that touch at least one of them.  Names are validated but not
    required to exist yet, so a consumer may subscribe before a collection is
    created.
    """
    if collections is None:
        return None
    if isinstance(collections, (str, bytes)):
        raise InvalidDocument("feed collection selection must be a collection of names")
    try:
        items = list(collections)
    except TypeError as exc:
        raise InvalidDocument("feed collection selection must be a collection of names") from exc
    if not items or len(items) > MAX_COLLECTIONS:
        raise InvalidDocument("feed collection selection must contain between 1 and %d names" % MAX_COLLECTIONS)
    return tuple(sorted({name(item) for item in items}))


def _decode_selection(stored):
    if stored is None:
        return None
    try:
        value = loads(stored)
    except (TypeError, ValueError) as exc:
        raise InvalidDocument("stored feed selection is malformed") from exc
    return _selection(value)


def _encode_selection(selection):
    return None if selection is None else dumps(list(selection))


def _public(selection):
    return None if selection is None else list(selection)


def _widen(previous, selection):
    """Whether the new selection includes collections the old one excluded."""
    if previous is None:
        return False
    if selection is None:
        return True
    return bool(set(selection) - set(previous))


def _touches(payload, selection):
    if selection is None:
        return True
    watched = set(selection)
    return any(operation.get("collection") in watched for operation in payload.get("operations", ()))


def _deliver(payload, selection):
    """Project a commit down to the watched collections.

    A selected event keeps the facts needed to explain the original commit it
    came from: the revision, digest and full operation count of the source
    commit, every collection that commit touched, and how many operations were
    withheld.  Without a selection the stored payload is returned untouched so
    the historical ``read`` contract is preserved exactly.
    """
    if selection is None:
        return payload
    watched = set(selection)
    operations = payload.get("operations", ())
    counts = {}
    for operation in operations:
        collection = operation.get("collection")
        counts[collection] = counts.get(collection, 0) + 1
    kept = [operation for operation in operations if operation.get("collection") in watched]
    return {**payload,
            "operations": kept,
            "selected": True,
            "partial": len(kept) != len(operations),
            "commit_operations": len(operations),
            "filtered_operations": len(operations) - len(kept),
            "touched_collections": list(counts),
            "selected_collections": [collection for collection in counts if collection in watched],
            "filtered_collections": [collection for collection in counts if collection not in watched],
            "scope": [{"collection": collection, "operations": count} for collection, count in counts.items()],
            "commit_digest": digest(payload)}


def _check_limit(limit):
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise InvalidDocument("feed limit must be between 1 and 1000 commits")


class ChangeFeed:
    def __init__(self, store):
        self.store = store

    def _row_or_none(self, connection, consumer):
        return connection.execute("SELECT ack,collections FROM consumers WHERE name=?", (consumer,)).fetchone()

    def _row(self, connection, consumer):
        row = self._row_or_none(connection, consumer)
        if row is None:
            raise NotFound("unknown consumer")
        return row

    def _scan(self, since, limit, selection):
        """Collect commit views for the next ``limit`` retained commits.

        ``limit`` bounds the commits examined, exactly as the historical feed
        did; a selection can therefore produce fewer events (or none) from one
        page.  Fully filtered commits are reported in ``skipped`` and are never
        left behind: ``next`` is the highest examined revision, so a consumer
        can acknowledge past them.  The next poll starts at ``next``, so
        relevant commits later in the stream are still reached.
        """
        with self.store.read() as connection:
            since = checked_revision(self.store, since)
            head, floor = int(self.store.get("head")), int(self.store.get("floor"))
            rows = connection.execute("SELECT rev,payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?",
                                      (since, limit)).fetchall()
        events, skipped, cursor_revision = [], [], int(since)
        for rev, stored in rows:
            cursor_revision = int(rev)
            payload = loads(stored)
            if _touches(payload, selection):
                events.append(_deliver(payload, selection))
            else:
                skipped.append(int(rev))
        return FeedPage(events, skipped=skipped, next=cursor_revision, examined=len(rows),
                        head=head, floor=floor,
                        selection=_public(selection),
                        complete=selection is None)

    def poll(self, since, limit=100, collections=None):
        """Page through commits after ``since`` with an optional selection.

        The returned :class:`FeedPage` carries ``next`` (the safe
        acknowledgement boundary), ``skipped`` (commits that touched no watched
        collection) and the watermark facts.  ``limit`` bounds examined
        commits, so pages stay bounded even when most writes belong to other
        collections.  Continue polling while ``next < head``; an empty
        ``events`` list with ``next > since`` still proves forward progress.
        """
        _check_limit(limit)
        selection = _selection(collections)
        return self._scan(since, limit, selection)

    def read(self, since, limit=100, collections=None):
        """Return commit views after ``since``.

        Without ``collections`` this keeps the original contract: a list of
        whole commit payloads, identical to the retained ``commits`` rows.
        With a selection, only commits touching a watched collection are
        returned, each projected to those operations while retaining the
        original commit's revision, digest, operation count and collection
        scope.  The list also exposes ``next`` and ``skipped`` like a
        :class:`FeedPage`.
        """
        return self.poll(since, limit, collections)

    def register(self, consumer, since=None, collections=None):
        name(consumer)
        selection = _selection(collections)
        with self.store.atomic() as connection:
            since = checked_revision(self.store, since)
            if connection.execute("SELECT 1 FROM consumers WHERE name=?", (consumer,)).fetchone():
                raise Conflict("consumer already registered")
            connection.execute("INSERT INTO consumers(name,ack,collections) VALUES (?,?,?)",
                               (consumer, since, _encode_selection(selection)))
        return since

    def position(self, consumer):
        with self.store.read() as connection:
            return self._row(connection, consumer)["ack"]

    def selection(self, consumer):
        with self.store.read() as connection:
            return _public(_decode_selection(self._row(connection, consumer)["collections"]))

    def describe(self, consumer):
        """Report the durable facts a reopening consumer resumes from.

        ``resumable`` is false when the stored acknowledgement is older than
        the retained floor; such a consumer must explicitly reattach instead of
        assuming no relevant commit was compacted away.
        """
        with self.store.read() as connection:
            row = self._row(connection, consumer)
            ack, floor, head = int(row["ack"]), int(self.store.get("floor")), int(self.store.get("head"))
            return {"consumer": consumer, "ack": ack, "selection": _public(_decode_selection(row["collections"])),
                    "floor": floor, "head": head, "pending": max(0, head - ack),
                    "resumable": ack >= floor}

    def subscribe(self, consumer, collections, since=None):
        """Replace a consumer's watched set without regressing its position.

        The change is durable, so a reopened process keeps consuming the same
        collections.  Widening a selection cannot resurrect commits the
        consumer already acknowledged; ``widened`` reports when the consumer
        should rebuild or explicitly re-read retained history instead.
        """
        name(consumer)
        selection = _selection(collections)
        with self.store.atomic() as connection:
            row = self._row(connection, consumer)
            previous = _decode_selection(row["collections"])
            ack = int(row["ack"])
            if since is None:
                revision = ack
            else:
                revision = checked_revision(self.store, since)
                if revision < ack:
                    raise Conflict("consumer acknowledgement cannot move backwards")
            connection.execute("UPDATE consumers SET ack=?,collections=? WHERE name=?",
                               (revision, _encode_selection(selection), consumer))
            return {"consumer": consumer, "ack": revision,
                    "selection": _public(selection),
                    "widened": _widen(previous, selection), "floor": int(self.store.get("floor"))}

    def reattach(self, consumer, since=None, collections=UNSET):
        """Make a consumer resumable after history it needed was compacted.

        A live consumer holds the floor below its acknowledgement, so a real
        gap appears when the consumer row was released (or lost with a restore)
        and compaction then advanced past its remembered position.  This call
        recreates or advances the consumer at ``max(remembered, floor)`` and
        reports ``dropped``: the revisions between the remembered position and
        the new one that can no longer be read.  The acknowledgement never
        moves backwards, and a resumable consumer is left unchanged, so the
        call is safe to repeat on every reconnect.
        """
        name(consumer)
        selection = _selection(collections) if collections is not UNSET else UNSET
        with self.store.atomic() as connection:
            row = self._row_or_none(connection, consumer)
            floor, head = int(self.store.get("floor")), int(self.store.get("head"))
            if since is None:
                remembered = int(row["ack"]) if row is not None else floor
            else:
                if isinstance(since, bool) or not isinstance(since, int) or since < 0 or since > head:
                    raise InvalidDocument("read revision must be between zero and current head")
                remembered = since
                if row is not None and int(row["ack"]) > remembered:
                    raise Conflict("consumer acknowledgement cannot move backwards")
            target = max(remembered, floor)
            stored = selection if collections is not UNSET else (
                _decode_selection(row["collections"]) if row is not None else None)
            if row is None:
                connection.execute("INSERT INTO consumers(name,ack,collections) VALUES (?,?,?)",
                                   (consumer, target, _encode_selection(stored)))
            else:
                connection.execute("UPDATE consumers SET ack=?,collections=? WHERE name=?",
                                   (target, _encode_selection(stored), consumer))
            return {"consumer": consumer, "ack": target, "dropped": max(0, target - remembered),
                    "selection": _public(stored), "floor": floor, "head": head,
                    "created": row is None, "was_stale": remembered < floor}

    def pending(self, consumer, limit=100, collections=UNSET):
        """Read after a consumer's stored acknowledgement.

        This is the restart path: the durable acknowledgement and selection are
        used unless ``collections`` is supplied (``None`` explicitly means the
        whole-commit feed).  A consumer whose stored position predates the
        retained floor raises :class:`SnapshotTooOld`; use :meth:`reattach` to
        accept the loss instead of silently skipping changes.
        """
        _check_limit(limit)
        with self.store.read() as connection:
            row = self._row(connection, consumer)
            ack = int(row["ack"])
            stored = _decode_selection(row["collections"])
        selection = stored if collections is UNSET else _selection(collections)
        if ack < int(self.store.get("floor")):
            raise SnapshotTooOld("consumer position %d is older than retained floor %d; call feed.reattach to accept the gap"
                                 % (ack, int(self.store.get("floor"))))
        page = self._scan(ack, limit, selection)
        page.consumer = consumer
        page.ack = ack
        return page

    def acknowledge(self, consumer, revision):
        """Advance a consumer's durable position.

        The boundary should come from a page's ``next`` (or from ``poll`` /
        ``pending``), which never moves past a commit that was not examined and
        already accounts for filtered commits.  A backwards acknowledgement
        still fails.
        """
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            row = self._row(connection, consumer)
            if revision < row["ack"]:
                raise Conflict("consumer acknowledgement cannot move backwards")
            connection.execute("UPDATE consumers SET ack=? WHERE name=?", (revision, consumer))
        return revision

    def release(self, consumer):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM consumers WHERE name=?", (consumer,)).rowcount:
                raise NotFound("unknown consumer")
