# Wrapper image cache recipes.
#
# The executor pulls wrapper images into $PISA_DATA_DIR/sif at task-claim time
# via executor.image_cache. The cache is digest-addressed: each .sif is named
# after the image's manifest digest (resolved from an @sha256 URI or a registry
# HEAD probe), so the executor re-pulls only when a tag's digest moves, and a tag
# and its pinned digest share one file. `update-cache` runs that same pull path
# ahead of time so a later claim skips the pull.

# Load executor/.env so PISA_DATA_DIR resolves the same way the executor sees it.
set dotenv-load

pisa_data_dir := env_var_or_default("PISA_DATA_DIR", "/PISA_DATA_DIR")
sif_dir := pisa_data_dir / "sif"

# Registry the executor pulls wrappers from. Single edit site if the host moves.
zot := "zot.hcislab.org"

# Wrapper images to keep warm, read live from the DB (simulator + av rows'
# image_path.apptainer). This is the exact URI a task claim resolves, so a
# prefetch/purge keyed off it cannot drift from what claims actually pull —
# whether a row carries a moving :main/:native tag or a pinned @sha256 digest.
_wrapper-images:
    #!/usr/bin/env bash
    set -euo pipefail
    pg="${MANAGER_URL%/manager}/postgrest"
    for tbl in simulator av; do
        curl -sfL "$pg/$tbl?select=image_path" \
            | python3 -c 'import sys, json; [print(u) for r in json.load(sys.stdin) if (u := (r.get("image_path") or {}).get("apptainer"))]'
    done

# Prefetch / update wrapper images into the cache (digest-checked, writes the
# sidecar so a later task claim skips the pull). With no args, updates every
# wrapper the DB references (`_wrapper-images`); pass explicit URIs to scope it.
update-cache *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="$(just _wrapper-images)"
    fi
    uv run -m executor.image_cache $uris

# Same as update-cache but re-pulls even when the cached digest already matches.
update-cache-force *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="$(just _wrapper-images)"
    fi
    uv run -m executor.image_cache --force $uris

# Prune stale/duplicate cached images, keeping only the .sif files the current
# `wrapper_images` resolve to (their digest-addressed names). Removes every
# other .sif — e.g. the previous digest of a tag that has since moved. Keep-set
# names come from image_cache itself (--print-names) so they always match what a
# claim resolves. With the 6 current images pulled, 6 remain.
purge-wrappers:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    dir="{{sif_dir}}"
    if [[ ! -d "$dir" ]]; then
        echo "SIF dir not found: $dir" >&2
        exit 1
    fi
    declare -A keep=()
    while IFS= read -r name; do
        [[ -n "$name" ]] && keep["$name"]=1
    done < <(uv run -m executor.image_cache --print-names $(just _wrapper-images))
    if [[ "${#keep[@]}" -eq 0 ]]; then
        echo "Could not resolve any current image names; aborting to avoid deleting the cache." >&2
        exit 1
    fi
    kept=0
    removed=0
    for sif in "$dir"/*.sif; do
        name="$(basename "$sif")"
        if [[ -n "${keep[$name]:-}" ]]; then
            kept=$((kept + 1))
            continue
        fi
        rm -f -- "$sif"
        echo "  removed $name"
        removed=$((removed + 1))
    done
    echo "Purge complete: kept $kept current image(s), removed $removed stale image(s) in $dir"


# Resolve each wrapper's :main / :native tag to its current Docker Hub digest and
# pin the matching DB simulator/av row's image_path to that @sha256 ref, so the
# executor pulls an immutable image instead of a moving tag. The digest is Hub's
# authoritative one; the executor pulls via zot, which serves that same digest for
# every OCI image (all wrappers built with buildx). CAVEAT: pcla-wrapper is built
# without buildx (Docker schema2), so zot re-wraps it to OCI under a different
# digest and cannot serve the Hub digest — its pin only resolves once pcla ships
# the buildx workflow and republishes as OCI. PostgREST base is derived from
# MANAGER_URL (.env). This name→repo→tag map is the one thing not DB-derived: once
# a row is pinned to a digest the tag is gone, so it lives here. `name` is the key.
# Run after a freshly-pushed tag to advance the pin. DRY=1 only prints the lookup.
pin-digests:
    #!/usr/bin/env bash
    set -euo pipefail
    pg="${MANAGER_URL%/manager}/postgrest"
    # table:name:repo:tag
    rows=(
        "simulator:esmini:esmini-wrapper:main"
        "simulator:carla:carla-wrapper:main"
        "simulator:native-carla:carla-wrapper:native"
        "av:autoware:autoware-wrapper:main"
        "av:plant:pcla-wrapper:main"
        "av:carla-agent:carla-agent-wrapper:main"
    )
    for row in "${rows[@]}"; do
        IFS=: read -r table name repo tag <<<"$row"
        digest=$(skopeo inspect "docker://docker.io/tonychi/$repo:$tag" \
            | python3 -c 'import sys,json;print(json.load(sys.stdin)["Digest"])')
        ref="{{zot}}/tonychi/$repo@$digest"
        printf '%-14s %-22s %s\n' "$name" "$repo:$tag" "$digest"
        if [[ "${DRY:-}" == "1" ]]; then continue; fi
        curl -sfL -X PATCH "$pg/$table?name=eq.$name" \
            -H 'Content-Type: application/json' \
            -d "{\"image_path\":{\"docker\":\"$ref\",\"apptainer\":\"docker://$ref\"}}" >/dev/null
    done
    if [[ "${DRY:-}" == "1" ]]; then echo "(dry run — no rows written)"; else echo "Pinned ${#rows[@]} rows via $pg"; fi
