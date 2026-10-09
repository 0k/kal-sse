use std::fs;
use std::path::Path;

use similar::TextDiff;

fn read_project_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

fn readme_rust_block(readme: &str) -> &str {
    let (_, after_start) = readme
        .split_once("#+begin_src rust\n")
        .expect("README.org must contain one Rust source block");
    let (block, remainder) = after_start
        .split_once("#+end_src")
        .expect("README.org Rust source block must end");
    assert!(
        !remainder.contains("#+begin_src rust"),
        "README.org must contain exactly one Rust source block"
    );
    block
}

fn example_body(example: &str) -> &str {
    let mut body_start = 0;
    for line in example.split_inclusive('\n') {
        if line.starts_with("//!") {
            body_start += line.len();
        } else {
            break;
        }
    }
    let body = &example[body_start..];
    body.strip_prefix('\n')
        .expect("example documentation must be followed by one blank line")
}

fn assert_text_eq(actual: &str, expected: &str) {
    if actual != expected {
        let diff = TextDiff::from_lines(expected, actual);
        eprintln!("{}", diff.unified_diff().header("expected", "actual"));
        panic!("text differs");
    }
}

#[test]
fn readme_server_example_matches_the_compiled_axum_example() {
    let readme = read_project_file("README.org");
    let example = read_project_file("examples/axum_server.rs");

    assert_text_eq(readme_rust_block(&readme), example_body(&example));
}
