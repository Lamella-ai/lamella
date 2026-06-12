# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 one-time Paperless namespace migration.

Renames every ``Lamella_X`` tag and custom field in Paperless to
``Lamella:X``. Idempotent — once all legacy names are gone, the
function is a no-op. The startup wiring in ``main.py`` flips a
settings flag (``paperless_namespace_migration_completed``) after
the first successful run so subsequent boots skip this work
entirely.

Strategy per kind:

1. **Tags.** For each tag whose name matches ``Lamella_X``:
   - If ``Lamella:X`` already exists: for every document tagged
     ``Lamella_X``, also tag ``Lamella:X`` (skip when already
     present), then untag ``Lamella_X``. Then delete the
     ``Lamella_X`` tag.
   - Otherwise: PATCH the tag's name from ``Lamella_X`` to
     ``Lamella:X`` in place. On a 4xx fall through to the
     copy + remove path.

2. **Custom fields.** Same logic. In-place name PATCH first,
   copy-and-remove fallback if Paperless rejects the rename
   (Paperless's API support for renaming a custom field varies
   across versions).

The migration is wrapped in try/except by its caller so a Paperless
outage at boot never breaks startup. On error, the flag stays unset
and the next boot retries.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.paperless_bridge.lamella_namespace import (
    LAMELLA_NAMESPACE_PREFIX_LEGACY,
    LAMELLA_NAMESPACE_PREFIX_NEW,
    is_lamella_name,
    to_canonical,
)

log = logging.getLogger(__name__)


@dataclass
class NamespaceMigrationReport:
    """Per-run summary returned to the caller. The lifespan wiring
    inspects ``errors`` to decide whether to flip the
    ``paperless_namespace_migration_completed`` setting (only flip on
    a clean run; partial runs retry next boot)."""

    tags_renamed_in_place: int = 0
    tags_migrated_via_copy: int = 0
    fields_renamed_in_place: int = 0
    fields_migrated_via_copy: int = 0
    documents_retagged: int = 0
    errors: list[str] = field(default_factory=list)
    # For test/observability: list of (legacy_name, canonical_name)
    # pairs the migration touched. Stable order matches Paperless's
    # listing order (which is typically id-ordered).
    tag_renames: list[tuple[str, str]] = field(default_factory=list)
    field_renames: list[tuple[str, str]] = field(default_factory=list)

    def total_writes(self) -> int:
        """Number of write operations the migration performed
        (PATCHes + POSTs + DELETEs). Used by the idempotency tests
        to assert a no-op second run."""
        return (
            self.tags_renamed_in_place
            + self.tags_migrated_via_copy
            + self.fields_renamed_in_place
            + self.fields_migrated_via_copy
            + self.documents_retagged
        )


async def _patch_tag_name(
    client: PaperlessClient, tag_id: int, *, new_name: str,
) -> bool:
    """Try to rename a Paperless tag in-place. Returns True on
    success, False on a 4xx (caller falls through to the copy path).
    Network/5xx errors propagate as PaperlessError to the caller's
    outer try/except.
    """
    try:
        await client._patch(f"/api/tags/{tag_id}/", body={"name": new_name})
        return True
    except PaperlessError as exc:
        msg = str(exc)
        # The PaperlessError message wraps the status code + body; a
        # 4xx (validation, "name already taken", etc.) is the only
        # case where we want to fall through. 5xx is a transient
        # condition and should propagate so the migration retries
        # next boot.
        if " 4" in msg.split("returned")[-1][:8]:
            log.info(
                "ADR-0064: in-place rename of tag id=%d to %r refused "
                "by Paperless (%s) — falling back to copy+remove path",
                tag_id, new_name, msg,
            )
            return False
        raise


async def _patch_field_name(
    client: PaperlessClient, field_id: int, *, new_name: str,
) -> bool:
    """Try to rename a Paperless custom field in-place. Same return
    contract as :func:`_patch_tag_name`."""
    try:
        await client._patch(
            f"/api/custom_fields/{field_id}/", body={"name": new_name},
        )
        return True
    except PaperlessError as exc:
        msg = str(exc)
        if " 4" in msg.split("returned")[-1][:8]:
            log.info(
                "ADR-0064: in-place rename of custom field id=%d to %r "
                "refused by Paperless (%s) — falling back to copy+remove",
                field_id, new_name, msg,
            )
            return False
        raise


async def _create_tag_bypassing_shim(
    client: PaperlessClient, *, name: str, color: str = "#0d6efd",
) -> int:
    """Direct POST /api/tags/ — bypasses ``client.ensure_tag``'s
    backwards-compat shim. Required for the migration fallback path
    where ensure_tag would resolve a canonical name to the legacy
    id (because the legacy tag is still present)."""
    created = await client._post(
        "/api/tags/", body={"name": name, "color": color},
    )
    new_id = created.get("id")
    if not isinstance(new_id, int):
        raise PaperlessError(
            f"POST /api/tags/ returned no id for {name!r}: {created}"
        )
    client._invalidate_list_tags_cache()
    return new_id


async def _delete_tag(client: PaperlessClient, tag_id: int) -> None:
    import httpx as _httpx
    try:
        resp = await client._client.delete(f"/api/tags/{tag_id}/")
    except _httpx.HTTPError as exc:
        raise PaperlessError(f"DELETE /api/tags/{tag_id}/ failed: {exc}") from exc
    if resp.status_code >= 400 and resp.status_code != 404:
        raise PaperlessError(
            f"DELETE /api/tags/{tag_id}/ returned {resp.status_code}: "
            f"{resp.text[:200]}"
        )


async def _delete_field(client: PaperlessClient, field_id: int) -> None:
    import httpx as _httpx
    try:
        resp = await client._client.delete(
            f"/api/custom_fields/{field_id}/"
        )
    except _httpx.HTTPError as exc:
        raise PaperlessError(
            f"DELETE /api/custom_fields/{field_id}/ failed: {exc}"
        ) from exc
    if resp.status_code >= 400 and resp.status_code != 404:
        raise PaperlessError(
            f"DELETE /api/custom_fields/{field_id}/ returned "
            f"{resp.status_code}: {resp.text[:200]}"
        )


async def _migrate_tags(
    client: PaperlessClient, report: NamespaceMigrationReport,
) -> None:
    """Rename every Lamella_X tag to Lamella:X."""
    tags = await client.list_tags(force_refresh=True)
    # Find legacy-prefixed tags in deterministic order so logs and
    # tests are reproducible.
    legacy_tags = sorted(
        (name, tid) for name, tid in tags.items()
        if name.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY)
    )
    if not legacy_tags:
        log.info("ADR-0064: no legacy Lamella_ tags found — tag migration is a no-op")
        return

    for legacy_n, legacy_id in legacy_tags:
        canonical_n = to_canonical(legacy_n)
        if canonical_n == legacy_n:
            # to_canonical is a no-op when the prefix isn't legacy;
            # this branch shouldn't fire given the filter above but
            # defensive against future shape changes.
            continue
        canonical_id = tags.get(canonical_n)
        if canonical_id is not None:
            # Both forms exist — copy doc tagging then remove legacy.
            log.info(
                "ADR-0064: both %r and %r exist; copying doc taggings "
                "then deleting %r",
                legacy_n, canonical_n, legacy_n,
            )
            try:
                retagged = await _retag_documents(
                    client,
                    from_tag_id=legacy_id,
                    to_tag_id=canonical_id,
                )
                report.documents_retagged += retagged
                await _delete_tag(client, legacy_id)
                report.tags_migrated_via_copy += 1
                report.tag_renames.append((legacy_n, canonical_n))
                client._invalidate_list_tags_cache()
            except PaperlessError as exc:
                report.errors.append(
                    f"tag-copy {legacy_n} -> {canonical_n}: {exc}"
                )
            continue
        # Canonical doesn't exist — try in-place rename.
        try:
            renamed = await _patch_tag_name(
                client, legacy_id, new_name=canonical_n,
            )
        except PaperlessError as exc:
            report.errors.append(
                f"tag-rename {legacy_n} -> {canonical_n}: {exc}"
            )
            continue
        if renamed:
            report.tags_renamed_in_place += 1
            report.tag_renames.append((legacy_n, canonical_n))
            client._invalidate_list_tags_cache()
            log.info(
                "ADR-0064: renamed tag %r -> %r in place (id=%d)",
                legacy_n, canonical_n, legacy_id,
            )
            continue
        # Fallback: create canonical, retag docs, delete legacy.
        # NOTE: bypass ``ensure_tag`` here — its backwards-compat
        # shim would return the legacy id (since the legacy tag
        # still exists at this point in the migration) and we'd
        # end up "retagging" docs from the legacy id back to the
        # legacy id (a no-op) before deleting the only tag id
        # that the docs were referring to (data loss).
        try:
            new_id = await _create_tag_bypassing_shim(
                client, name=canonical_n,
            )
            retagged = await _retag_documents(
                client,
                from_tag_id=legacy_id,
                to_tag_id=new_id,
            )
            report.documents_retagged += retagged
            await _delete_tag(client, legacy_id)
            report.tags_migrated_via_copy += 1
            report.tag_renames.append((legacy_n, canonical_n))
            client._invalidate_list_tags_cache()
        except PaperlessError as exc:
            report.errors.append(
                f"tag-fallback {legacy_n} -> {canonical_n}: {exc}"
            )


async def _retag_documents(
    client: PaperlessClient, *, from_tag_id: int, to_tag_id: int,
) -> int:
    """For every doc tagged ``from_tag_id``, ensure it's also tagged
    ``to_tag_id`` and remove ``from_tag_id``. Returns the number of
    documents PATCHed."""
    if from_tag_id == to_tag_id:
        return 0
    count = 0
    async for doc in client.iter_documents({"tags__id__all": str(from_tag_id)}):
        current = list(doc.tags or [])
        if from_tag_id not in current:
            # Server-side filter said yes but local check disagrees;
            # skip rather than waste a PATCH.
            continue
        new_tags = [t for t in current if t != from_tag_id]
        if to_tag_id not in new_tags:
            new_tags.append(to_tag_id)
        try:
            await client.patch_document(doc.id, tags=new_tags)
            count += 1
        except PaperlessError as exc:
            log.warning(
                "ADR-0064: doc %d retag %d->%d failed: %s",
                doc.id, from_tag_id, to_tag_id, exc,
            )
    return count


async def _migrate_fields(
    client: PaperlessClient, report: NamespaceMigrationReport,
) -> None:
    """Rename every Lamella_X custom field to Lamella:X."""
    # Force fresh cache so a stale cached listing doesn't hide a
    # legacy field that was renamed by hand in Paperless.
    client._field_cache = None
    cache = await client._load_field_cache()
    legacy_fields = sorted(
        (f.name, f.id) for f in cache.values()
        if f.name.startswith(LAMELLA_NAMESPACE_PREFIX_LEGACY)
    )
    if not legacy_fields:
        log.info(
            "ADR-0064: no legacy Lamella_ custom fields found — "
            "field migration is a no-op"
        )
        return
    name_to_id = {f.name: f.id for f in cache.values()}
    for legacy_n, legacy_id in legacy_fields:
        canonical_n = to_canonical(legacy_n)
        if canonical_n == legacy_n:
            continue
        canonical_id = name_to_id.get(canonical_n)
        if canonical_id is not None:
            # Both exist — Paperless custom-field values are stored
            # per-doc-per-field-id. Move every doc's value from the
            # legacy field id to the canonical field id, then delete
            # the legacy field.
            log.info(
                "ADR-0064: both %r and %r custom fields exist; copying "
                "values then deleting %r",
                legacy_n, canonical_n, legacy_n,
            )
            try:
                moved = await _move_field_values(
                    client,
                    from_field_id=legacy_id,
                    to_field_id=canonical_id,
                )
                report.documents_retagged += moved
                await _delete_field(client, legacy_id)
                report.fields_migrated_via_copy += 1
                report.field_renames.append((legacy_n, canonical_n))
                client._field_cache = None
            except PaperlessError as exc:
                report.errors.append(
                    f"field-copy {legacy_n} -> {canonical_n}: {exc}"
                )
            continue
        # Canonical doesn't exist — try in-place rename.
        try:
            renamed = await _patch_field_name(
                client, legacy_id, new_name=canonical_n,
            )
        except PaperlessError as exc:
            report.errors.append(
                f"field-rename {legacy_n} -> {canonical_n}: {exc}"
            )
            continue
        if renamed:
            report.fields_renamed_in_place += 1
            report.field_renames.append((legacy_n, canonical_n))
            client._field_cache = None
            log.info(
                "ADR-0064: renamed custom field %r -> %r in place (id=%d)",
                legacy_n, canonical_n, legacy_id,
            )
            continue
        # Fallback: create canonical, copy values, delete legacy.
        try:
            new_field = await client.create_custom_field(
                name=canonical_n, data_type="string",
            )
            moved = await _move_field_values(
                client,
                from_field_id=legacy_id,
                to_field_id=new_field.id,
            )
            report.documents_retagged += moved
            await _delete_field(client, legacy_id)
            report.fields_migrated_via_copy += 1
            report.field_renames.append((legacy_n, canonical_n))
            client._field_cache = None
        except PaperlessError as exc:
            report.errors.append(
                f"field-fallback {legacy_n} -> {canonical_n}: {exc}"
            )


async def _move_field_values(
    client: PaperlessClient, *, from_field_id: int, to_field_id: int,
) -> int:
    """For every doc whose custom_fields list carries ``from_field_id``,
    re-write the list to use ``to_field_id`` instead. Returns the
    number of documents PATCHed."""
    if from_field_id == to_field_id:
        return 0
    count = 0
    async for doc in client.iter_documents(
        {"custom_fields__id__in": str(from_field_id)}
    ):
        # Be defensive — Paperless may not honor the filter on every
        # version, so verify the doc actually carries the field.
        existing: dict[int, Any] = {
            cf.field: cf.value for cf in doc.custom_fields
        }
        if from_field_id not in existing:
            continue
        value = existing.pop(from_field_id)
        # Don't clobber an existing canonical value if one happens to
        # already be set; the canonical write wins per ADR-0064.
        if to_field_id not in existing:
            existing[to_field_id] = value
        body_fields = [
            {"field": fid, "value": val} for fid, val in existing.items()
        ]
        try:
            await client.patch_document(doc.id, custom_fields=body_fields)
            count += 1
        except PaperlessError as exc:
            log.warning(
                "ADR-0064: doc %d field-move %d->%d failed: %s",
                doc.id, from_field_id, to_field_id, exc,
            )
    return count


async def run_namespace_migration(
    client: PaperlessClient,
) -> NamespaceMigrationReport:
    """Rename every ``Lamella_X`` tag and custom field in Paperless
    to ``Lamella:X``.

    Idempotent: once all ``Lamella_X`` tags/fields are gone, the
    function is a no-op (zero writes, empty report).

    See module docstring for the rename strategy + fallback path.
    """
    report = NamespaceMigrationReport()
    log.info("ADR-0064: starting Paperless namespace migration")
    try:
        await _migrate_tags(client, report)
    except PaperlessError as exc:
        # _migrate_tags catches per-rename errors into the report;
        # this catches a top-level failure (e.g. list_tags itself
        # failed). Log + continue so fields still get a try.
        report.errors.append(f"tag-list: {exc}")
    try:
        await _migrate_fields(client, report)
    except PaperlessError as exc:
        report.errors.append(f"field-list: {exc}")
    log.info(
        "ADR-0064: migration done — tags(in_place=%d, copy=%d), "
        "fields(in_place=%d, copy=%d), docs_touched=%d, errors=%d",
        report.tags_renamed_in_place, report.tags_migrated_via_copy,
        report.fields_renamed_in_place, report.fields_migrated_via_copy,
        report.documents_retagged, len(report.errors),
    )
    return report


__all__ = [
    "NamespaceMigrationReport",
    "run_namespace_migration",
]
