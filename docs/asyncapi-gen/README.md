## Generate File in Version 2.4.0
```
cd asyncapi-gen
go generate ./...
```

## Convert to Version 3.0.0
copy/paste to https://studio.asyncapi.com
a dialog to convert should pop up. if not, you must clear your cookies/local-storage.

alternatively, you could use the `asyncapi convert` cli described in https://www.asyncapi.com/docs/migration/migrating-to-v3

## The committed document does not follow `go generate`

`docs/asyncapi.json` is the **3.0.0** document, produced by the conversion step
above. The generator writes **2.4.0** next to itself, and that file is
gitignored, so running `go generate ./...` alone leaves the committed document
untouched.

The document is reflected out of Go types, `model.DeviceTypeCommand` among them,
so a bump of `models/go` changes it without anyone editing anything here: a
field added to `models.ContentVariable` appears in `ModelsContentVariable`. No CI
step regenerates and compares, so the committed document can claim an outdated
model and nothing reports it. After a model bump, regenerate and convert rather
than editing the 3.0.0 file by hand.

This module pins its own dependencies and points at the parent with `replace`,
so its `go.mod` has to be kept in step with the parent's — otherwise a later run
drifts for two reasons at once and the diff no longer shows which.
