# -*- mode: shell-script -*-
##
## Vendored from 0k-pkg (``cargo/autogen.d/20-org-to-md.sh``).
## Refresh with ``pkg vendor``; to keep local edits, add a line
## ``## pkgcmd: local-override`` in the first five lines.
##
## Generates ``README.md`` from ``README.org`` for crates.io, and
## appends the changelog to it: cargo has no dedicated changelog
## field, the README is the only file crates.io renders.
##
## Tables are emitted as GFM pipe tables (``+pipe_tables``): plain
## ``commonmark`` falls back to raw HTML tables, which crates.io strips.
##
## Needs ``pandoc`` and ``gitchangelog``.  Without them this step is
## skipped with a warning (a fresh clone can still build); the release
## pipeline sets ``AUTOGEN_STRICT=1`` which makes them mandatory.
##

## Lua filter: convert org-mode ``:no_run yes`` (and friends) on a
## rust block to the ```` ```rust,no_run ```` fence rustdoc expects
## instead of pandoc's ``{.rust .no_run}`` attribute syntax.
## Lua patterns are literal, not shell expansions.
# shellcheck disable=SC2016
RUSTDOC_ATTRS_LUA='
function CodeBlock(el)
  if el.classes[1] == "rust" then
    local extra = {}
    for k, v in pairs(el.attributes) do
      local key = k:gsub("-", "_")
      if key == "no_run" or key == "norun" then
        table.insert(extra, "no_run")
      elseif key == "ignore" then
        table.insert(extra, "ignore")
      elseif key == "compile_fail" then
        table.insert(extra, "compile_fail")
      elseif key == "should_panic" then
        table.insert(extra, "should_panic")
      end
    end
    if #extra > 0 then
      local info = "rust," .. table.concat(extra, ",")
      return pandoc.RawBlock("markdown", "```" .. info .. "\n" .. el.text .. "```\n")
    end
  end
  return el
end
function Pandoc(doc)
  if doc.meta.title then
    doc = doc:walk {
      Header = function(el)
        el.level = el.level + 1
        return el
      end
    }
    table.insert(doc.blocks, 1, pandoc.Header(1, doc.meta.title))
  end
  return doc
end
'

## Shift ATX headings, but not examples inside backtick/tilde fences.
org_to_md_shift_changelog() {
    awk '
    {
      text = $0
      sub(/^   |^  |^ /, "", text)
      if (text ~ /^```+/ || text ~ /^~~~+/) {
        char = substr(text, 1, 1)
        run = text
        if (char == "`") sub(/[^`].*$/, "", run)
        else sub(/[^~].*$/, "", run)
        if (!fence) { fence = char; width = length(run) }
        else if (char == fence && length(run) >= width &&
                 substr(text, length(run) + 1) ~ /^[[:space:]]*$/) fence = ""
        print
        next
      }
      if (!fence && $0 ~ /^[ ]?[ ]?[ ]?#+([[:space:]]|$)/)
        sub(/#/, "##")
      print
    }' "$1"
}

if [ -f README.org ]; then
    depends_soft pandoc || {
        echo "README.md not regenerated (fine for building, required for release)." >&2
        return 0
    }
    lua_filter=$(mktemp)
    printf '%s\n' "$RUSTDOC_ATTRS_LUA" > "$lua_filter"
    pandoc README.org -f org -t commonmark+pipe_tables --lua-filter="$lua_filter" -o README.md.tmp || {
        rm -f "$lua_filter" README.md.tmp
        return 1
    }
    rm -f "$lua_filter"

    if depends_soft gitchangelog; then
        if [ ! -e .gitchangelog.rc ]; then
            echo "No .gitchangelog.rc found: changelog not appended to README.md." >&2
        else
            echo "" >> README.md.tmp
            echo "" >> README.md.tmp
            changelog_tmp=$(mktemp) || return 1
            gitchangelog > "$changelog_tmp" || {
                rm -f "$changelog_tmp" README.md.tmp
                return 1
            }
            if grep -qi '^#+TITLE:' README.org; then
                org_to_md_shift_changelog "$changelog_tmp" >> README.md.tmp
            else
                cat "$changelog_tmp" >> README.md.tmp
            fi
            rm -f "$changelog_tmp"
        fi
    elif [ -f CHANGELOG.md ]; then
        echo "" >> README.md.tmp
        echo "" >> README.md.tmp
        if grep -qi '^#+TITLE:' README.org; then
            org_to_md_shift_changelog CHANGELOG.md >> README.md.tmp
        else
            cat CHANGELOG.md >> README.md.tmp
        fi
    fi

    if [ -f README.md ] && diff README.md README.md.tmp > /dev/null; then
        echo "No changes in README.md" >&2
        rm README.md.tmp
    else
        echo "Updating README.md" >&2
        mv README.md.tmp README.md
    fi
fi
