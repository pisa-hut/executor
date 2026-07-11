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

# Wrapper images to keep warm — must match each DB row's image_path.apptainer
# verbatim so a prefetch caches under the same filename a task claim resolves.
# All use the moving :main / :native tag on the zot pull-through; the executor's
# digest-checked .sif sidecar re-pulls only when a tag moves, so "latest" stays
# current without editing the DB. Served via zot (not docker.io direct) so the
# executor's unauthenticated HEAD probe can resolve the tag's digest — docker.io
# 401s it. carla-wrapper has two live builds: :main (carla) and :native (native-carla).
wrapper_images := "docker://zot.hcislab.org/tonychi/carla-wrapper:main docker://zot.hcislab.org/tonychi/carla-wrapper:native docker://zot.hcislab.org/tonychi/carla-agent-wrapper:main docker://zot.hcislab.org/tonychi/autoware-wrapper:main docker://zot.hcislab.org/tonychi/esmini-wrapper:main docker://zot.hcislab.org/tonychi/pcla-wrapper:main"

# Prefetch / update wrapper images into the cache (digest-checked, writes the
# sidecar so a later task claim skips the pull). With no args, updates the
# `wrapper_images` list; pass explicit URIs to update just those.
update-cache *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="{{wrapper_images}}"
    fi
    uv run -m executor.image_cache $uris

# Same as update-cache but re-pulls even when the cached digest already matches.
update-cache-force *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="{{wrapper_images}}"
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
    done < <(uv run -m executor.image_cache --print-names {{wrapper_images}})
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

# Deprecated: superseded by `update-cache`. Forwards for now.
pull-wrappers *uris:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "pull-wrappers is deprecated; use 'just update-cache {{uris}}'" >&2
    just update-cache {{uris}}
