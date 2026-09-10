# Event descriptions are written elsewhere

This service reads event descriptions and never writes one. That is easy to
misread from the inside, because the collection is configured here and a write
function for it exists, and it decides how a field added to `model.EventDesc`
has to be handled.

## Scope

Holds for `model.EventDesc` and its readers in this repository: `pkg/eventrepo`
in both modes, `pkg/marshaller`, and `pkg/worker`. It is about where a
descriptor comes from and what follows for a field added to it.

**Not this if** the question is what the aspect, function or characteristic
fields of a descriptor *mean*. How they resolve to a path inside a message is
the marshaller's subject, and the rules for a criteria naming several aspects
belong to the device-repository's model. Also not this if a descriptor is simply
absent at runtime: that is a question about the service that fills the
collection, and all this document settles is that the service is not this one.
Neighbouring cases beyond these two have not been checked.

## Both modes read, neither writes

`Mongo.SetEventDescription` in `pkg/eventrepo/cloud/mongo/descriptions.go` is
defined and called nowhere in this repository. Every descriptor function that is
actually used is a read — by device and service, by device group, by import, by
service, by event id — plus one delete by deployment id.

- **Cloud mode** reads the collection named by
  `cloud_event_repo_mongo_table` and `cloud_event_repo_mongo_desc_collection`.
  Another service writes the rows into it.
- **Fog mode** (`pkg/eventrepo/fog`) fetches descriptors over HTTP from
  `GET /event-descriptions` of the
  [process sync client](https://github.com/SENERGY-Platform/mgw-process-sync-client),
  which mirrors them from the cloud.

Neither path builds an `EventDesc` out of a deployment, so nothing here can
derive a descriptor field from the process model it originally came from. The
comment on `EventDesc.ServiceForMarshaller` names the event-manager as the
component that fills that field, which is the only writer this repository refers
to at all.

## What that means for a new field

Three consequences, and the third is the one that makes a change here only half a
change.

**A field has to be additive.** The rows already stored were written against the
previous struct, and they are not migrated. In json and bson a new field
therefore carries `omitempty`, so a descriptor that never had it round-trips
unchanged and an older reader sees the payload it knows. `aspect_ids` is written
that way.

**A compatibility fold belongs on the read side.** Where a field replaces an
older one, this service cannot normalise on write, because it does not write.
The fold has to happen where the value is used, on every entry path, and it has
to prefer the new field while falling back to the old one. `EventDesc.AspectId`
is deprecated in favour of `EventDesc.AspectIds`, and `EventDesc.GetAspectIds()`
is that fold — a single aspect id folded into a one-element list, so that
everything behind it evaluates the list only. Nothing should read `AspectId`
directly.

**The field stays inert until the writing side fills it.** Adding `AspectIds`
here makes multi-aspect descriptors *evaluable*; it does not make them *exist*.
Until the service that writes the collection emits the field, every descriptor
arrives with the deprecated single value, the fold turns it into a one-element
list, and behaviour is bit-for-bit what it was. So a change of this kind cannot
be verified against production data from this repository alone, and the tests
that cover it are the ones asserting that the deprecated spelling keeps working.
