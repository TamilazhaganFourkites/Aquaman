"""Regression: the Bash-guard hook must block a recursive search rooted at a filesystem top-level
(the EXE-0417bc97 / MM-14457 stall — a coder ran `grep -rln ... /` and hung the station 30+ min),
while never blocking a search scoped to the repo worktree."""
import asyncio

from src.ocean_pipeline.agents import unscoped_root_search, _guard_bash


def test_blocks_whole_disk_searches():
    assert unscoped_root_search('grep -rln "def port_of_loading?" / | grep -i ocean | head') == "/"
    assert unscoped_root_search("find / -name '*.rb'") == "/"
    assert unscoped_root_search("grep -R foo /Users") == "/Users"
    assert unscoped_root_search("rg pat /usr") == "/usr"
    assert unscoped_root_search("grep -rn x ~") == "~"


def test_blocks_evasions():
    # root-glob (shell expands /* to every top-level dir) + wrapper/env prefixes must not slip past.
    assert unscoped_root_search("grep -r x /*") == "/*"
    assert unscoped_root_search("grep -r x /*/") == "/*/"
    assert unscoped_root_search("sudo grep -r x /") == "/"
    assert unscoped_root_search("time grep -r x /") == "/"
    assert unscoped_root_search("FOO=1 grep -r x /") == "/"
    assert unscoped_root_search("xargs grep -r x /") == "/"


def test_allows_scoped_searches():
    assert unscoped_root_search('grep -rn "def get_stop" app/models/ocean') is None
    assert unscoped_root_search("grep -rn foo .") is None
    assert unscoped_root_search("find . -name '*.rb'") is None
    assert unscoped_root_search('find "$WORKTREE_DIR" -type f') is None
    # absolute path INTO the run's worktree is fine — only top-level system roots are dangerous
    assert unscoped_root_search("rg pat /tmp/ocean-pipeline/EXE-x/workspace") is None
    assert unscoped_root_search("grep -n foo /etc/hosts") is None   # not recursive
    assert unscoped_root_search("ls /") is None                     # not a search tool


def test_no_false_positive_when_grepping_FOR_a_path_literal():
    # grep's FIRST bare positional is the PATTERN, not a path — searching FOR a path string, scoped to a
    # relative dir, must be ALLOWED (the coder greps for path literals constantly).
    assert unscoped_root_search('grep -rn "/" .') is None
    assert unscoped_root_search('grep -rn "/etc" app/models/ocean') is None
    assert unscoped_root_search('grep -rn "/System" app/') is None
    assert unscoped_root_search('rg "/usr" src/') is None
    # ...but the real dangerous root as the PATH arg is still caught even when the pattern looks path-like
    assert unscoped_root_search('grep -rn "/etc" /') == "/"


def _run_hook(cmd):
    hook = _guard_bash()
    return asyncio.run(hook({"tool_name": "Bash", "tool_input": {"command": cmd}}, "t", None))


def test_hook_denies_root_search_and_allows_scoped():
    denied = _run_hook('grep -rln "x" /')
    assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "scans the whole machine" in denied["hookSpecificOutput"]["permissionDecisionReason"]
    assert _run_hook("grep -rn x app/") == {}          # scoped -> allowed (empty = no decision)
    assert _run_hook("echo hi") == {}                  # non-search -> allowed
