"""Unit tests for the deterministic quality gate's pure helpers (src/ocean_pipeline/quality.py).

These deliberately hit REAL `git`, `ruby`, `gofmt` and `compile()` rather than mocks. The failure mode
this module exists to prevent is a gate that checks nothing and reports clean, and a mocked test
reproduces that state perfectly while proving nothing.

Run: pytest tests/test_quality.py
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from ocean_pipeline import quality


def _w(d: Path, name: str, text: str) -> Path:
    p = d / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return p


def _git_repo(tmp_path: Path) -> Path:
    """A real repo with a remote-tracking base but NO origin/HEAD — the shape a fresh clone can have,
    and the one that decides whether the gate is inert on the real path."""
    d = tmp_path / "repo"
    d.mkdir(parents=True)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    g("init", "-q", "-b", "develop")
    g("config", "user.email", "t@example.com")
    g("config", "user.name", "t")
    _w(d, "keep.go", "package m\n")
    _w(d, "gone.go", "package m\n")
    g("add", "-A"); g("commit", "-qm", "base")
    g("update-ref", "refs/remotes/origin/develop", "HEAD")   # base exists, origin/HEAD does not
    g("checkout", "-qb", "MM-1/fix")
    return d


# --------------------------------------------------------------------------- changed-file derivation
def test_changed_files_against_a_real_repo(tmp_path):
    """THE highest-value test here. An empty changed-file list makes the whole gate inert while every
    surface reads green — so this exercises the real hazards: origin/HEAD unset (name fallback),
    deletions (must be excluded, since checking a path that no longer exists always fails), and a
    modification alongside an addition."""
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "added.rb", "puts 1\n")
    _w(d, "keep.go", "package m\n// touched\n")
    (d / "gone.go").unlink()
    g("add", "-A"); g("commit", "-qm", "work")

    assert quality.base_ref(d, 30) == "origin/develop", "name fallback must fire when origin/HEAD is unset"
    files, why = quality.changed_files(d, 30, 200)
    names = sorted(f.name for f in files)
    # Which mechanism guards a deleted path here: MEASURED, not assumed. Dropping
    # `--diff-filter=ACMR` FAILS this test; dropping `is_file()` does not — the not-on-disk reason
    # added later now fires on a deletion. An earlier version of this comment had it exactly
    # backwards; quality.py states it correctly, and the two must agree.
    assert names == ["added.rb", "keep.go"]
    assert not (d / "gone.go").exists(), "fixture sanity: the file really was deleted"
    assert why == ""


def test_three_dot_diff_excludes_commits_that_landed_on_the_base_after_forking(tmp_path):
    """Both `git diff` flags were unpinned — a judge mutated `...` to `..` and `--diff-filter=ACMR`
    away with the whole suite still green. Two-dot would drag in every file the BASE branch changed
    after this branch forked, so the gate would flag files the coder never touched."""
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "mine.rb", "puts 1\n")
    g("add", "-A"); g("commit", "-qm", "mine")
    # Someone else MODIFIES an existing file on the base after we forked. It must be a modification,
    # not an addition: a two-dot diff renders a base-only ADD as a deletion, which --diff-filter=ACMR
    # would drop anyway — so an added file cannot tell the two forms apart.
    g("checkout", "-q", "develop")
    _w(d, "keep.go", "package m\n// THEIRS\n")
    g("add", "-A"); g("commit", "-qm", "theirs")
    g("update-ref", "refs/remotes/origin/develop", "HEAD")
    g("checkout", "-q", "MM-1/fix")

    names = sorted(f.name for f in quality.changed_files(d, 30, 200)[0])
    assert "mine.rb" in names
    assert "keep.go" not in names, \
        "three-dot must diff against the MERGE BASE — two-dot drags in the base branch's own changes"


def test_changed_files_reports_infra_failure_rather_than_an_empty_pass(tmp_path):
    """A non-repo must produce a REASON, never a silent empty list — an empty list is indistinguishable
    from 'nothing changed' and is exactly how a gate goes inert."""
    d = tmp_path
    files, why = quality.changed_files(d, 30, 200)
    assert files == [] and why, "a missing .git must be could-not-run, not a clean pass"


def test_changed_files_cap_is_reported_not_silent(tmp_path):
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    for i in range(6):
        _w(d, f"f{i}.py", "x = 1\n")
    g("add", "-A"); g("commit", "-qm", "many")
    files, why = quality.changed_files(d, 30, 3)
    assert len(files) == 3 and "cap" in why.lower(), "truncation must be stated, never silent"


# --------------------------------------------------------------------------- per-language checks
@pytest.mark.skipif(not shutil.which("gofmt"), reason="gofmt not installed")
def test_go_parse_error_blocks_but_formatting_does_not(tmp_path):
    """Measured on real repos: 43 of 184 .go files in ocean-service are ALREADY gofmt-dirty on an
    untouched checkout (booking-service 12/61). Blocking on formatting would therefore kill legitimate
    runs on a file the coder barely touched. Parse errors are a different thing entirely."""
    broken = _w(tmp_path, "broken.go", "package m\nfunc G( {\n}\n")
    unfmt = _w(tmp_path, "unfmt.go", "package m\nfunc F() {\n x := 1\n _ = x\n}\n")
    clean = _w(tmp_path, "ok.go", "package m\n\nfunc F() {\n\tx := 1\n\t_ = x\n}\n")

    got, why, _n = quality.check_go(tmp_path, [broken], 30)
    assert why == "" and len(got) == 1
    assert got[0]["severity"] == quality.BLOCKING and got[0]["line"] == 2

    got, _, _n = quality.check_go(tmp_path, [unfmt], 30)
    assert len(got) == 1 and got[0]["severity"] == quality.ADVISORY, "formatting must NOT block"

    assert quality.check_go(tmp_path, [clean], 30)[0] == [], "no false positive on a clean file"


def test_python_syntax_blocks_and_writes_no_pycache(tmp_path):
    """`python -m py_compile` would drop __pycache__ into the coder's worktree, which then shows up in
    the reviewer's `git status` and possibly the pushed diff. In-process compile() writes nothing."""
    _w(tmp_path, "pyproject.toml", "[project]\nname='x'\n")   # mark it a real Python project
    bad = _w(tmp_path, "b.py", "def f(:\n  pass\n")
    good = _w(tmp_path, "g.py", "def f():\n    return 1\n")
    got, why, _n = quality.check_python(tmp_path, [bad], 30)
    assert why == "" and got and got[0]["severity"] == quality.BLOCKING
    assert quality.check_python(tmp_path, [good], 30)[0] == []
    assert not (tmp_path / "__pycache__").exists(), "the gate must not write into the coder's tree"


def test_yaml_is_reported_but_never_blocks(tmp_path):
    """BLOCK ONLY WHEN THE VALIDATOR IS THE CONSUMER. ruby -c / gofmt / compile() are the parsers that
    actually load these files in production. PyYAML is not — Ruby uses Psych, Go its own library,
    Spring SnakeYAML. A judge found PyYAML rejecting two SHIPPED environment-configuration
    settings.yml files over literal tabs that Psych accepts, which at MAX=1 would have ended a run
    with no PR for a line the coder never wrote."""
    pytest.importorskip("yaml")
    bad = _w(tmp_path, "b.yml", "a:\n  - x\n b: [\n")
    good = _w(tmp_path, "g.yml", "a:\n  b: 1\n")
    got, _, _n = quality.check_yaml(tmp_path, [bad], 30)
    assert got, "a malformed YAML must still be REPORTED to the coder"
    assert got[0]["severity"] == quality.ADVISORY, "...but must never hard-stop the run"
    from ocean_pipeline import schemas
    assert not schemas.is_blocking_finding(got[0])
    assert quality.check_yaml(tmp_path, [good], 30)[0] == []


# --------------------------------------------------------------------------- ruby + its version guard
def test_duplicate_anchor_does_not_mask_a_later_syntax_error(tmp_path):
    """PyYAML ABORTS the compose at the first error, so a bare `continue` on 'duplicate anchor'
    validated nothing after it while still crediting the file. Three shipped environment-configuration
    files carry duplicate anchors — exactly where a coder-introduced error downstream would sail
    through. The re-parse with a redefinition-tolerant loader is what closes it."""
    pytest.importorskip("yaml")
    ok = _w(tmp_path, "dup_ok.yml", "a: &x 1\nb: &x 2\nc: 3\n")
    bad = _w(tmp_path, "dup_then_broken.yml", "a: &x 1\nb: &x 2\nc:\n  - d\n e: [\n")
    aliased = _w(tmp_path, "dup_alias.yml", "a: &x 1\nb: &x 2\nc: *x\n")

    got, why, n = quality.check_yaml(tmp_path, [ok], 30)
    assert got == [] and n == 1, "a duplicate anchor alone is not an error"
    got, why, n = quality.check_yaml(tmp_path, [bad], 30)
    assert got, "a REAL syntax error after a duplicate anchor must still be found"
    assert n == 1
    got, _, _n = quality.check_yaml(tmp_path, [aliased], 30)
    assert got == [], "popping the anchor must not break alias resolution"


def test_ruby_version_guard_compares_major_minor_only(tmp_path):
    """An earlier draft compared FULL versions, justified by 'host ruby is 2.7.5 while ocean repos
    target 3.x'. That was inferred and is false — every ocean Ruby repo targets 2.7.x (ocean-worker
    2.7.5, mcuw 2.7.5, tracking-service 2.7.7, global_worker 2.7.7). A patch-level compare would have
    refused the host on two of the four over 2.7.5-vs-2.7.7, manufacturing 'unverified' across half the
    Ruby surface for zero grammar difference."""
    def repo(decl: str) -> Path:
        d = tmp_path / f"r{abs(hash(decl))}"
        d.mkdir()
        if decl:
            (d / ".ruby-version").write_text(decl + "\n")
        return d

    assert quality.ruby_version_ok(repo("ruby-2.7.5"), host_version="2.7.5")[0] is True
    assert quality.ruby_version_ok(repo("ruby-2.7.7"), host_version="2.7.5")[0] is True, \
        "a patch-level gap must NOT refuse the host"
    ok, why = quality.ruby_version_ok(repo("ruby-3.3.0"), host_version="2.7.5")
    assert ok is False and why, "a MAJOR.MINOR mismatch must refuse"
    ok, why = quality.ruby_version_ok(repo(""), host_version="2.7.5")
    assert ok is False and "declares no ruby version" in why


@pytest.mark.skipif(not shutil.which("ruby"), reason="ruby not installed")
def test_ruby_falls_back_to_the_host_only_when_the_grammar_matches(tmp_path):
    """And when it refuses, it must report could-not-run — never an empty finding list that reads
    as clean."""
    d = tmp_path / "rb"
    d.mkdir()
    # An IMPOSSIBLE version, not 3.3.0 — on a Ruby 3.3.x host the guard would PERMIT, check_ruby
    # would find the real error, and `assert got == []` would break. The sibling test at
    # test_checked_count_is_asserted_on_every_checker already uses this trick.
    _w(d, ".ruby-version", "ruby-9.9.9\n")
    bad = _w(d, "b.rb", "def f\n  1\n")
    got, why, _n = quality.check_ruby(d, [bad], 30, container="")
    assert got == [] and why, "a refused interpreter is could-not-run, not a pass"
    # On a ruby-less host this test otherwise passes via the "ruby is not on PATH" branch — a
    # different mechanism than its name claims. Pin the one it is actually about.
    assert "does not match" in why, f"must refuse on the VERSION mismatch: {why!r}"


@pytest.mark.skipif(not shutil.which("ruby"), reason="ruby not installed")
def test_ruby_syntax_error_is_caught_with_a_line_number(tmp_path):
    host = subprocess.run(["ruby", "-e", "print RUBY_VERSION"], capture_output=True, text=True).stdout.strip()
    d = tmp_path / "rb2"
    d.mkdir()
    _w(d, ".ruby-version", f"ruby-{host}\n")        # match the host so the guard permits it
    bad = _w(d, "b.rb", "def f\n  1\n")             # missing `end`
    got, why, _n = quality.check_ruby(d, [bad], 30, container="")
    assert why == "" and len(got) == 1
    assert got[0]["severity"] == quality.BLOCKING and got[0].get("line")
    good = _w(d, "g.rb", "def f\n  1\nend\n")
    assert quality.check_ruby(d, [good], 30, container="")[0] == []


# --------------------------------------------------------------------------- run_all + the invariant
def test_run_all_distinguishes_derived_from_checked(tmp_path):
    """The anti-inertness pair. A judge replayed this design against a real completed run
    (eta-worker/MM-14312): 8 files derived, 0 checked (7 .java with Java off, 1 .yaml), findings empty,
    route proceed — inert while looking clean. An invariant keyed on `derived == 0` never fires there,
    so the caller must gate on CHECKED."""
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "Main.java", "class Main {}\n")           # no checker with Java off
    _w(d, "notes.md", "# hi\n")                     # no checker at all
    g("add", "-A"); g("commit", "-qm", "uncheckable")

    findings, why, checked, derived, uncovered = quality.run_all([d], "", timeout=30, cap=200, java_on=False)
    assert derived == 2 and checked == 0, "this is the real inert shape"
    assert findings == []
    # NOTE: an uncovered FILE TYPE is deliberately NOT a could-not-run reason — funnelling it there
    # made the node shout "DID NOT FULLY RUN" on 48% of real ocean commits (any diff touching a .md or
    # .json), and an alarm that fires on half of all runs stops being read. The inertness signal is
    # `checked == 0` with files derived, which the NODE turns into could-not-run; see
    # test_quality_gate_node_invariant_fires_when_nothing_is_checked in test_graph.py.


def test_run_all_reports_no_repo_as_could_not_run():
    findings, why, checked, derived, uncovered = quality.run_all([], "", timeout=30, cap=200, java_on=False)
    assert findings == [] and checked == 0 and derived == 0 and why


def test_run_all_survives_a_checker_that_raises(tmp_path, monkeypatch):
    """An unexpected fault in one language must degrade to could-not-run for that slice, never take
    down the gate and never read as a pass."""
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "x.py", "x = 1\n")
    g("add", "-A"); g("commit", "-qm", "py")

    def boom(*a, **kw):
        raise RuntimeError("checker exploded")

    monkeypatch.setitem(quality._CHECKERS, "python", boom)
    findings, why, checked, derived, uncovered = quality.run_all([d], "", timeout=30, cap=200, java_on=False)
    assert derived == 1 and checked == 0 and findings == []
    assert "checker exploded" in why


def test_checked_count_is_asserted_on_every_checker(tmp_path, monkeypatch):
    """The third return value (`checked`) is the anti-inertness invariant's ONLY input, and it shipped
    with zero assertions — a judge mutated it to `len(files)` in three checkers and all 225 tests still
    passed. A checker that over-reports coverage defeats `checked == 0` silently."""
    # NOTE: the DISCRIMINATING assertions below must use a case where `checked != len(files)` —
    # asserting an EQUAL case cannot tell a correct count from `len(files)`, and three such mutants
    # survived a full 228-test run because the first version of this test did exactly that. The two
    # equal-case asserts that remain (empty list, and the happy Python path) are sanity checks, not
    # discriminators, and are labelled as such.

    # Go: one readable + one gofmt CANNOT read -> 1 of 2 credited.
    a = _w(tmp_path, "a.go", "package m\n")
    missing = tmp_path / "gone.go"          # never created
    class _P:
        returncode, stdout = 2, ""
        stderr = f"open {missing}: no such file or directory\n"
    monkeypatch.setattr(quality, "_run", lambda *x, **k: _P())
    monkeypatch.setattr(quality.shutil, "which", lambda _n: "/usr/bin/gofmt")
    assert quality.check_go(tmp_path, [a, missing], 30)[2] == 1, \
        "a file gofmt could not open must NOT be credited as checked"
    monkeypatch.undo()
    assert quality.check_go(tmp_path, [], 30)[2] == 0        # sanity, not a discriminator

    # Python: 0 in a non-Python repo, len(files) in a real one.
    _w(tmp_path, "Gemfile", "source 'x'\n")
    assert quality.check_python(tmp_path, [_w(tmp_path, "s.py", "x=1\n")], 30)[2] == 0
    proj = tmp_path / "pp"; _w(proj, "pyproject.toml", "[project]\nname='x'\n")
    assert quality.check_python(proj, [_w(proj, "s.py", "x=1\n")], 30)[2] == 1  # sanity, not a discriminator

    # YAML: a file that only parses AFTER the template fallback is NOT credited.
    pytest.importorskip("yaml")
    plain = _w(tmp_path, "p.yml", "a: 1\n")
    tmpl = _w(tmp_path, "t.yml", "a: {{ .V }}\nb: [\n")     # templated AND unparseable -> skipped
    got, why, n = quality.check_yaml(tmp_path, [plain, tmpl], 30)
    assert n == 1, f"only the genuinely-parsed file may be credited (n={n})"

    # Java: spotless is a FORMATTER. Force the success path (mvn present, exit 0) so this asserts the
    # RULE rather than accidentally passing because mvn is absent on this machine.
    _w(tmp_path, "pom.xml", "<project><build><plugins><plugin>"
                            "<artifactId>spotless-maven-plugin</artifactId></plugin></plugins></build></project>")
    class _OK:
        returncode, stdout, stderr = 0, "", ""
    monkeypatch.setattr(quality, "_run", lambda *x, **k: _OK())
    monkeypatch.setattr(quality.shutil, "which", lambda _n: "/usr/bin/mvn")
    java_files = [_w(tmp_path, "A.java", "class A {"), _w(tmp_path, "B.java", "class B {")]
    got, why, n = quality.check_java(tmp_path, java_files, 30)
    assert n == 0, f"a formatter must never claim syntax coverage (got {n} for {len(java_files)} files)"
    assert why, "and it must say the files went syntax-unchecked"
    monkeypatch.undo()

    # Ruby: an interpreter that becomes unreachable PART WAY credits only what it managed.
    rb = tmp_path / "rb"; _w(rb, ".ruby-version", "ruby-9.9.9\n")
    assert quality.check_ruby(rb, [_w(rb, "a.rb", "puts 1\n")], 30, container="")[2] == 0
    calls = {"n": 0}
    class _Half:
        stdout = ""
        def __init__(self):
            calls["n"] += 1
            self.returncode = 0 if calls["n"] == 1 else 1
            self.stderr = "" if calls["n"] == 1 else "Error response from daemon: No such container"
    monkeypatch.setattr(quality, "_run", lambda *x, **k: _Half())
    rb2 = tmp_path / "rb2"
    files2 = [_w(rb2, "a.rb", "puts 1\n"), _w(rb2, "b.rb", "puts 2\n"), _w(rb2, "c.rb", "puts 3\n")]
    got, why, n = quality.check_ruby(rb2, files2, 30, container="c")
    assert n == 1, f"only the file checked before the container died may be credited (got {n} of 3)"
    assert why and not got


def test_ruby_bytes_reach_the_interpreter_unaltered(tmp_path):
    """`stdin_bytes.decode("utf-8", "replace")` under text=True turned an invalid byte sequence into
    U+FFFD, which Ruby accepts — so a file `ruby -c` REJECTS was reported '1 file(s) clean'. The
    module's headline contract, inverted."""
    if not shutil.which("ruby"):
        pytest.skip("ruby not installed")
    host = subprocess.run(["ruby", "-e", "print RUBY_VERSION"], capture_output=True, text=True).stdout.strip()
    _w(tmp_path, ".ruby-version", f"ruby-{host}\n")
    bad = tmp_path / "invalid.rb"
    bad.write_bytes(b'# encoding: utf-8\nx = "\xff\xfe"\nputs x\n')
    ground = subprocess.run(["ruby", "-c", str(bad)], capture_output=True, text=True)
    if ground.returncode == 0:
        pytest.skip("this ruby accepts the sequence; nothing to assert")
    got, why, n = quality.check_ruby(tmp_path, [bad], 30, container="")
    assert got, "a file the real interpreter rejects must NOT pass the gate"


def test_empty_ruby_file_does_not_hang_on_inherited_stdin(tmp_path):
    """`if stdin_bytes` is falsy for b"", which passed input=None and let the child INHERIT stdin.
    `ruby -c` then blocked on the terminal for the whole timeout, abandoned every remaining file, and
    could swallow keystrokes at a LangGraph interrupt() approval gate."""
    if not shutil.which("ruby"):
        pytest.skip("ruby not installed")
    host = subprocess.run(["ruby", "-e", "print RUBY_VERSION"], capture_output=True, text=True).stdout.strip()
    _w(tmp_path, ".ruby-version", f"ruby-{host}\n")
    empty = tmp_path / "empty.rb"; empty.write_bytes(b"")
    after = _w(tmp_path, "after.rb", "def f\n 1\n")          # a REAL error behind the empty file

    # The child must get an EXPLICIT empty stdin, not inherit ours. Under pytest's fd capture the
    # parent's fd 0 is already /dev/null, so a buggy `if stdin_bytes` variant reads instant EOF and
    # the test passes either way — a judge proved exactly that (both mutants survived 184 tests).
    # Point fd 0 at a real OPEN PIPE for the duration, which is what a live terminal looks like.
    import os, time
    r_fd, w_fd = os.pipe()
    saved = os.dup(0)
    try:
        os.dup2(r_fd, 0)
        t0 = time.time()
        got, why, n = quality.check_ruby(tmp_path, [empty, after], 10, container="")
        elapsed = time.time() - t0
    finally:
        os.dup2(saved, 0)
        for fd in (saved, r_fd, w_fd):
            os.close(fd)

    assert elapsed < 5, (f"an empty file must not block on inherited stdin (took {elapsed:.1f}s "
                         f"— `if stdin_bytes` is falsy for b'' and passes input=None)")
    assert got, "the real syntax error behind the empty file must still be found"
    assert n == 2


def test_java_is_actually_dispatched(tmp_path):
    """`.java` was missing from _SUFFIX_LANG, so check_java, _CHECKERS["java"], run_all's java branch,
    the `java_on` parameter and OCEAN_PIPELINE_QUALITY_GATE_JAVA were ALL unreachable — and with them
    the "N Java file(s) were NOT checked" loud-channel signal. A judge found 2 unparseable .java plus
    a valid .rb reporting "1 file(s) clean", silently."""
    assert quality.language_of(Path("A.java")) == "java"
    assert "java" in quality._CHECKERS

    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "A.java", "class A {")          # unparseable, but spotless is a formatter
    g("add", "-A"); g("commit", "-qm", "java")
    findings, why, checked, derived, unc = quality.run_all([d], "", timeout=30, cap=200, java_on=False)
    assert derived == 1 and checked == 0
    # Must name the KNOB, not merely contain "Java ... NOT checked" — check_java's own
    # "declares no spotless plugin" message also matches that, so a judge's mutant inverting the
    # java_on branch survived the weaker assertion.
    assert "QUALITY_GATE_JAVA" in why, f"the java_on branch must be the one that fired: {why!r}"
    # ...and with the knob ON the java_on branch must NOT fire; check_java takes over instead.
    findings, why_on, checked_on, _d, _u = quality.run_all([d], "", timeout=30, cap=200, java_on=True)
    assert "QUALITY_GATE_JAVA" not in why_on, f"java_on=True must dispatch to check_java: {why_on!r}"
    assert checked_on == 0, "spotless is a formatter — it may never claim syntax coverage"


def test_unreadable_files_are_could_not_run_never_findings(tmp_path):
    """check_ruby already treated an unreadable file as could-not-run; check_yaml BLOCKED on it
    (contradicting its own never-block rule) and check_python both blocked AND credited it. At MAX=1
    that ends a run over a file the coder cannot fix — the fail-closed-on-infra inversion this module
    exists to avoid."""
    pytest.importorskip("yaml")
    import os
    y = _w(tmp_path, "a.yml", "a: 1\n")
    _w(tmp_path, "pyproject.toml", "[project]\nname='x'\n")
    py = _w(tmp_path, "a.py", "x = 1\n")
    os.chmod(y, 0o000); os.chmod(py, 0o000)
    try:
        if os.access(y, os.R_OK):          # running as root: chmod is not a barrier
            pytest.skip("cannot make a file unreadable as this user")
        got, why, n = quality.check_yaml(tmp_path, [y], 30)
        assert got == [], "an unreadable YAML must not produce a finding"
        assert why and n == 0, "it must be could-not-run, and must NOT be credited"

        got, why, n = quality.check_python(tmp_path, [py], 30)
        assert got == [], "an unreadable .py must not produce a finding"
        assert why and n == 0, "and must NOT be credited as checked"
    finally:
        os.chmod(y, 0o644); os.chmod(py, 0o644)


def test_non_ascii_paths_are_not_silently_dropped(tmp_path):
    """`git diff --name-only` octal-quotes non-ASCII paths under the default core.quotepath, the
    quoted string does not exist on disk, and `is_file()` dropped it SILENTLY — a broken file simply
    vanished from the gate, with no alarm at all if any ordinary file was also in the diff. `-z` emits
    raw, unquoted, NUL-separated names."""
    d = _git_repo(tmp_path)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    _w(d, "café_broken.rb", "def f\n 1\n")
    _w(d, "plain.rb", "puts 1\n")
    g("add", "-A"); g("commit", "-qm", "unicode")
    files, why = quality.changed_files(d, 30, 200)
    names = sorted(f.name for f in files)
    assert "café_broken.rb" in names, f"a non-ASCII path must survive derivation: {names}"
    assert why == ""


def test_go_and_ruby_are_actually_dispatched():
    """The same class as the `.java` dead-path defect: deleting an extension from _SUFFIX_LANG makes
    a whole language silently uncheckable, and a judge found `.go` had no test pinning it."""
    for name, lang in (("a.go", "go"), ("a.rb", "ruby"), ("a.py", "python"),
                       ("a.yml", "yaml"), ("a.yaml", "yaml"), ("A.java", "java"),
                       ("Gemfile", "ruby"), ("Rakefile", "ruby")):
        assert quality.language_of(Path(name)) == lang, name
    assert quality.language_of(Path("notes.md")) == ""
    for lang in ("go", "python", "yaml", "java"):
        assert lang in quality._CHECKERS, lang


@pytest.mark.skipif(not shutil.which("gofmt"), reason="gofmt not installed")
def test_gofmt_diagnostic_survives_a_colon_in_the_path(tmp_path):
    """Splitting the whole stderr line on ':' truncated a path containing a colon, producing a
    CRITICAL against a file that does not exist — while the coder prompt says 'fix EXACTLY these
    files'. The path is now resolved by matching what we asked for."""
    weird = tmp_path / "we:ird"
    broken = _w(weird, "b.go", "package m\nfunc G( {\n}\n")
    got, why, n = quality.check_go(tmp_path, [broken], 30)
    assert len(got) == 1 and got[0]["severity"] == quality.BLOCKING
    assert got[0]["file"].endswith("we:ird/b.go"), f"path was mangled: {got[0]['file']!r}"
    assert got[0].get("line") == 2


def test_ruby_empty_output_exit_is_not_a_syntax_error(tmp_path):
    """`msg` used to default to the literal 'syntax error', which matched the allowlist — so a
    non-zero exit with COMPLETELY empty output (exit 137 on a memory-capped container, observed for
    real) was reported as a parse failure on a file that parses fine."""
    class _P:
        returncode, stdout, stderr = 137, "", ""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(quality, "_run", lambda *a, **kw: _P())
    try:
        f = _w(tmp_path, "a.rb", "puts 1\n")
        got, why, n = quality.check_ruby(tmp_path, [f], 30, container="c")
        assert got == [], "an empty-output exit must not be reported as a syntax error"
        assert why and n == 0
    finally:
        monkey.undo()


def test_a_repo_that_derives_nothing_is_reported_even_with_a_healthy_sibling(tmp_path):
    """The node's invariant is a run-level AGGREGATE. A repo deriving 0 files adds 0 to BOTH counters,
    so one healthy sibling silenced the alarm completely for a repo whose clone was left on the base
    branch — its broken files never checked, nothing said. The reason has to be raised per-repo,
    where the fact is known."""
    def mk(name, on_base):
        d = tmp_path / name
        d.mkdir(parents=True)

        def g(*a):
            return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

        g("init", "-q", "-b", "develop"); g("config", "user.email", "t@t"); g("config", "user.name", "t")
        _w(d, "pyproject.toml", "[project]\nname='x'\n")     # so .py is actually checked
        _w(d, "seed.py", "x = 1\n")
        g("add", "-A"); g("commit", "-qm", "base")
        g("update-ref", "refs/remotes/origin/develop", "HEAD")
        g("checkout", "-qb", "MM-1/fix")
        _w(d, "added.py", "y = 2\n")
        g("add", "-A"); g("commit", "-qm", "w")
        if on_base:
            g("checkout", "-q", "develop")       # derives nothing
        return d

    quiet, busy = mk("quiet", on_base=True), mk("busy", on_base=False)
    findings, why, checked, derived, unc = quality.run_all([quiet, busy], "", timeout=30, cap=200,
                                                           java_on=False)
    assert checked > 0, "the healthy sibling really was checked"
    assert "quiet" in why and "NO changed files" in why, \
        f"the repo that derived nothing must still be named: {why!r}"


def test_findings_use_severities_that_actually_canonicalize():
    """schemas.normalize_severity returns "" for anything unmapped, so a free-text severity would
    produce a finding that blocks NOTHING. The constants must survive that canonicalizer."""
    from ocean_pipeline import schemas
    assert schemas.is_blocking_finding({"severity": quality.BLOCKING}) is True
    assert schemas.is_blocking_finding({"severity": quality.ADVISORY}) is False


def test_python_is_skipped_in_a_non_python_repo(tmp_path):
    """Every ocean service repo is Ruby/Go/Java, and their stray .py files are PYTHON 2 helper scripts.
    A judge measured a py3 compile() flagging 6 of 6 stray .py in the ocean service repos — a blocking CRITICAL on 100% of the .py
    files this checker is most likely to meet. Without a target interpreter we cannot judge them."""
    _w(tmp_path, "Gemfile", "source 'https://rubygems.org'\n")     # a Ruby repo
    py2 = _w(tmp_path, "app/scripts/legacy.py", "print 'hello'\n")  # valid py2, invalid py3
    got, why, _n = quality.check_python(tmp_path, [py2], 30)
    assert got == [] and "not a Python project" in why

    proj = tmp_path / "realpy"
    _w(proj, "pyproject.toml", "[project]\nname='x'\n")
    bad = _w(proj, "b.py", "def f(:\n")
    got, why, _n = quality.check_python(proj, [bad], 30)
    assert why == "" and got and got[0]["severity"] == quality.BLOCKING


def test_templated_and_tagged_yaml_do_not_produce_false_blocks(tmp_path):
    """Measured on untouched checkouts: a naive safe_load blocks 16% of ocean-worker's .yml, 13% of
    global_worker's, 9.8% of environment-configuration's, and 3 of 63 real MERGED commits. ERB and Go
    templates are template SOURCE, not YAML documents; custom tags are valid YAML that safe_load
    refuses to CONSTRUCT. Neither is a syntax error."""
    pytest.importorskip("yaml")
    erb = _w(tmp_path, "shoryuken.yml", "concurrency: <%= ENV['N'] %>\nqueues:\n  - [q, 1]\n")
    helm = _w(tmp_path, "deployment.yaml", "spec:\n  replicas: {{ .Values.replicas }}\n")
    tagged = _w(tmp_path, "cassette.yml", "body:\n  string: !binary |-\n    aGVsbG8=\n")
    got, why, n = quality.check_yaml(tmp_path, [erb, helm, tagged], 30)
    assert got == [], f"valid-but-templated/tagged YAML must not block: {got}"
    # A templated skip is NOT a could-not-run reason — it is simply uncredited. Reporting it as one
    # made a batch of 5 good files + 1 templated file announce "CHECKED 0 of 6", firing the
    # anti-inertness alarm on a run that had in fact been checked (judge review).
    assert why == "", f"a templated skip must not read as could-not-run: {why!r}"
    # ...and files that merely CONTAIN a marker but parse cleanly are still checked: skipping on the
    # marker alone discarded 104 of 214 real templated ocean files for nothing.
    assert n >= 2, f"only genuinely-unparseable templated files should go uncredited (n={n})"

    broken = _w(tmp_path, "broken.yml", "a:\n  - x\n b: [\n")
    got, _, _n = quality.check_yaml(tmp_path, [broken], 30)
    assert got, "a REAL syntax error must still be reported (advisory — see the validator/consumer rule)"


def test_go_io_failure_is_could_not_run_not_a_silent_clean_pass(tmp_path, monkeypatch):
    """gofmt exits non-zero on I/O failure writing `open <path>: <errno>`, which has only three
    colon-parts and was silently dropped — leaving ([], "") while run_all credited every file as
    CHECKED. A judge produced a genuinely-unparseable .go coming back '1 file(s) clean'."""
    target = tmp_path / "y.go"
    monkeypatch.setattr(quality.shutil, "which", lambda _n: "/usr/bin/gofmt")

    # (a) generic failure with no parseable diagnostic -> could-not-run, nothing credited
    class _Generic:
        returncode, stdout, stderr = 2, "", "gofmt: fatal internal error\n"
    monkeypatch.setattr(quality, "_run", lambda *a, **kw: _Generic())
    got, why, n = quality.check_go(tmp_path, [target], 30)
    assert got == [] and "NOT checked" in why and n == 0

    # (b) BOTH I/O verbs must be recognised — macOS gofmt says `stat`, Linux says `open`, and the
    # path must match the file we asked for (a judge's stub used /x/y.go, which matched nothing, so
    # this branch was never exercised).
    for verb in ("open", "stat"):
        class _IO:
            returncode, stdout = 2, ""
            stderr = f"{verb} {target}: no such file or directory\n"
        monkeypatch.setattr(quality, "_run", lambda *a, **kw: _IO())
        got, why, n = quality.check_go(tmp_path, [target], 30)
        assert n == 0, f"a file gofmt could not {verb} must not be credited"
        assert "could not read" in why, f"the {verb} variant must reach the unreadable branch: {why!r}"


def test_ruby_unreachable_interpreter_is_could_not_run_not_a_parse_error(tmp_path, monkeypatch):
    """prep_container starts the container three stations upstream; if it dies in that window every
    changed .rb came back CRITICAL 'does not parse: Error response from daemon: No such container' —
    failing CLOSED on infrastructure, the inverse of this module's contract."""
    class _P:
        returncode, stdout = 1, ""
        stderr = "Error response from daemon: No such container: ocean-x\n"
    monkeypatch.setattr(quality, "_run", lambda *a, **kw: _P())
    f = _w(tmp_path, "a.rb", "puts 1\n")
    got, why, _n = quality.check_ruby(tmp_path, [f], 30, container="ocean-x")
    assert got == [], "an unreachable interpreter must not be reported as a syntax error"
    assert why and "NOT checked" in why


def test_base_ref_handles_a_default_branch_containing_a_slash(tmp_path):
    """`head.rsplit('/', 1)[-1]` turned `refs/remotes/origin/release/v1` into `origin/v1` — which
    either exits 128 or, if an unrelated `origin/v1` exists, silently diffs against the WRONG base."""
    d = tmp_path / "r"
    d.mkdir(parents=True)

    def g(*a):
        return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

    g("init", "-q", "-b", "release/v1"); g("config", "user.email", "t@t"); g("config", "user.name", "t")
    _w(d, "a.txt", "x\n"); g("add", "-A"); g("commit", "-qm", "base")
    g("update-ref", "refs/remotes/origin/release/v1", "HEAD")
    g("update-ref", "refs/remotes/origin/v1", "HEAD")       # the decoy the old code would have picked
    g("symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/release/v1")
    assert quality.base_ref(d, 30) == "origin/release/v1"


def test_a_path_git_names_but_that_is_not_on_disk_is_reported(tmp_path, monkeypatch):
    """`errors="replace"` stops a non-UTF-8 path CRASHING the gate, but the replaced name does not
    exist on disk — so without a reason the file silently vanishes, and with any ordinary file also in
    the diff no alarm fires at all. That is verbatim the failure `-z` was added to stop."""
    d = tmp_path / "repo"
    (d / ".git").mkdir(parents=True)
    real = _w(d, "ok.py", "x = 1\n")

    class _P:
        returncode, stderr = 0, ""
        stdout = "ok.py\0caf�_broken.rb\0"          # one real, one undecodable

    monkeypatch.setattr(quality, "base_ref", lambda *a, **kw: "origin/develop")
    monkeypatch.setattr(quality.subprocess, "run", lambda *a, **kw: _P())
    files, why = quality.changed_files(d, 30, 200)
    assert files == [real]
    assert why and "not on disk" in why, f"a vanished path must be reported, not dropped: {why!r}"


def test_multi_repo_findings_are_prefixed_with_their_repo(tmp_path):
    """Two repos can hold the same relative path, and the coder prompt says "fix EXACTLY these
    files" — an unqualified `a.py` names neither."""
    def mk(name):
        d = tmp_path / name
        d.mkdir(parents=True)

        def g(*a):
            return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True)

        g("init", "-q", "-b", "develop"); g("config", "user.email", "t@t"); g("config", "user.name", "t")
        _w(d, "pyproject.toml", "[project]\nname='x'\n"); _w(d, "seed.py", "x = 1\n")
        g("add", "-A"); g("commit", "-qm", "b"); g("update-ref", "refs/remotes/origin/develop", "HEAD")
        g("checkout", "-qb", "MM-1/fix")
        _w(d, "a.py", "def f(:\n")
        g("add", "-A"); g("commit", "-qm", "w")
        return d

    one, two = mk("repo-one"), mk("repo-two")
    findings, _why, _c, _d, _u = quality.run_all([one, two], "", timeout=30, cap=200, java_on=False)
    files = sorted(f["file"] for f in findings)
    assert files == ["repo-one/a.py", "repo-two/a.py"], f"unqualified in a multi-repo run: {files}"
    # ...and a SINGLE-repo run stays unprefixed (relative to that repo).
    findings, _w2, _c2, _d2, _u2 = quality.run_all([one], "", timeout=30, cap=200, java_on=False)
    assert [f["file"] for f in findings] == ["a.py"]


def test_an_unreadable_workspace_entry_does_not_discard_the_other_repos(tmp_path):
    """`Path.exists()` re-raises EACCES (it only swallows ENOENT/ENOTDIR/EBADF/ELOOP). With it
    outside `_add`'s try, one unreadable entry propagated to the broad `except OSError` around the
    child loop and DISCARDED every sibling sorting after it — a repo whose .rb does not parse
    vanished with no alarm."""
    import os
    for name in ("a_ok", "z_worker"):
        (tmp_path / name / ".git").mkdir(parents=True)
    blocked = tmp_path / "m_blocked"
    blocked.mkdir()
    os.chmod(blocked, 0o000)
    try:
        if os.access(blocked, os.R_OK):
            pytest.skip("cannot make a directory unreadable as this user")
        found = sorted(p.name for p in quality.repo_dirs("", tmp_path))
        assert found == ["a_ok", "z_worker"], \
            f"a sibling sorting AFTER an unreadable entry must survive: {found}"
    finally:
        os.chmod(blocked, 0o755)


def test_the_file_cap_still_applies_when_a_path_is_missing(tmp_path, monkeypatch):
    """The not-on-disk early return preceded the cap check, so a 51-file diff with one missing path
    returned 50 files against a cap of 10 — a regression introduced by the fix above it."""
    d = tmp_path / "repo"
    (d / ".git").mkdir(parents=True)
    real = [_w(d, f"f{i}.py", "x = 1\n") for i in range(50)]
    names = [f.name for f in real] + ["ghost.py"]

    class _P:
        returncode, stderr = 0, ""
        stdout = "\0".join(names) + "\0"

    monkeypatch.setattr(quality, "base_ref", lambda *a, **kw: "origin/develop")
    monkeypatch.setattr(quality.subprocess, "run", lambda *a, **kw: _P())
    files, why = quality.changed_files(d, 30, 10)
    assert len(files) == 10, f"the cap must still apply: got {len(files)}"
    assert "not on disk" in why and "cap" in why, f"both facts must be reported: {why!r}"


def test_python_reads_bytes_so_a_file_cpython_rejects_cannot_pass(tmp_path):
    """`read_text(errors="replace")` turned an invalid sequence into U+FFFD, which compiles fine — so
    a file the real interpreter REJECTS passed and was credited. Verbatim the Ruby defect that `_run`'s
    docstring calls the headline contract inverted, fixed there and left live here."""
    _w(tmp_path, "pyproject.toml", "[project]\nname='x'\n")
    bad = tmp_path / "bad.py"
    bad.write_bytes(b'# -*- coding: utf-8 -*-\ns = "\xff\xfe"\n')
    try:
        compile(bad.read_bytes(), str(bad), "exec")
        pytest.skip("this CPython accepts the sequence; nothing to assert")
    except (SyntaxError, ValueError):
        pass
    got, why, n = quality.check_python(tmp_path, [bad], 30)
    assert got, "a file CPython rejects must NOT pass the gate"
