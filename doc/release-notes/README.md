# Release notes

One file per release, named after its tag: `v0.2.0.md` holds the notes of
`v0.2.0`. The content is the body of the GitHub release, in Markdown.

`.github/workflows/release.yml` picks the file up when the tag is pushed. A
tag with no matching file still produces a release, with the notes GitHub
generates from the merged pull requests — which is a poorer summary than a
written one, so the file is worth writing before the tag is pushed.

The shape the releases so far have used:

```markdown
One paragraph on what the release is about.

**[API reference](https://xcomart.github.io/libcmdbm/api/)** ·
**[Project page](https://xcomart.github.io/libcmdbm/)**

## Added
## Fixed
## Changed
## Known limitations
```

Only the sections that have something to say are included. "Known
limitations" carries what is still missing or untested, so that the release
page answers the question before it is asked.
