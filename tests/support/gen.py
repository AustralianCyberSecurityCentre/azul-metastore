"""Generate fake binary/status/plugin events."""

import copy
import hashlib

from azul_bedrock import models_network as azm
from azul_bedrock.models_restapi import binaries_data as bedr_bdata

from azul_metastore.common.utils import md5
from azul_metastore.models import basic_events

g1_1, g2_1, g3_1, g1_12, g2_12, g3_12 = secs = (
    "LOW TLP:CLEAR",
    "MEDIUM REL:APPLE",
    "MEDIUM MOD1 REL:APPLE",
    "LOW TLP:GREEN",
    "MEDIUM REL:APPLE REL:BEE",
    "MEDIUM MOD1 REL:APPLE REL:BEE",
)


def _patch(patch, doc):
    for k, v in patch.items():
        if isinstance(v, dict) and k in doc:
            _patch(v, doc[k])
        else:
            doc[k] = v
    return doc


def gen_binary_data(x: bytes, label: azm.DataLabel = azm.DataLabel.CONTENT):
    """Return fake dispatcher response for posting data."""
    return azm.Datastream(
        identify_version=1,
        label=label,
        sha256=hashlib.sha256(x).hexdigest(),
        sha512=hashlib.sha512(x).hexdigest(),
        sha1=hashlib.sha1(x).hexdigest(),
        md5=hashlib.md5(x).hexdigest(),
        size=len(x),
        file_format_legacy="GIF",
        magic="magical",
        mime="mimish",
        file_format="image/gif",
        tlsh="T1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ssdeep="3072:fakessdeepr6QyyjEfQvAqnf5BtZKDLeM93IbsDV:QVUWEh6nyjEIvAqnf5BOnL1V",
        file_extension=".gif",
    )


def gen_binary_data_as_binary_data(x: bytes, label: azm.DataLabel = azm.DataLabel.CONTENT, filename: str = ""):
    return bedr_bdata.BinaryData(**gen_binary_data(b"contents", azm.DataLabel.CONTENT).model_dump(), filename=filename)


def data(patch: dict = None, *, hash=None) -> dict:
    if not patch:
        patch = {}
    if hash is not None:
        _patch(
            {
                "sha256": hash,
                "sha512": f"{hash:0>128}",
                "sha1": f"{hash[:40]:0>40}",
                "md5": f"{hash[:32]:0>32}",
            },
            patch,
        )
    return _patch(
        patch,
        dict(
            identify_version=1,
            label="content",
            magic="ASCII text",
            mime="text/plain",
            file_format_legacy="Text",
            file_format="text/plain",
            file_extension="txt",
            ssdeep="1:1:1",
            size=1024,
            tlsh="T1" + "0" * 70,
            md5=f"{'ab':0>32}",
            sha1=f"{'ab':0>40}",
            sha256=f"{'ab':0>64}",
            sha512=f"{'ab':0>128}",
        ),
    )


def path(patch: dict = None, *, eid=None, authornv=None, authoru=None, summary_file_size=1024, action=None) -> dict:
    if not patch:
        patch = {}
    if authornv is not None:
        _patch({"author": {"name": authornv[0], "version": authornv[1]}}, patch)
    if authoru is not None:
        _patch({"author": {"category": "user", "name": authoru, "version": "1"}}, patch)
    if eid is not None:
        _patch({"sha256": eid}, patch)
    if action is not None:
        _patch({"action": action}, patch)
    return _patch(
        patch,
        dict(
            sha256="generic_entity",
            size=summary_file_size,
            file_format_legacy="Text",
            file_format="text/plain",
            author=dict(
                category="plugin",
                name="generic_author",
                version="2021-01-01T12:00:00+00:00",
                security=g1_1,
            ),
            relationship={"random": "data", "action": "extracted", "label": "within"},
            action=azm.BinaryAction.Sourced,
            timestamp="2021-01-01T12:00:00+00:00",
        ),
    )


def feature(patch: dict = None, *, fv=None, fvt=None) -> dict:
    if not patch:
        patch = {}
    if fv is not None:
        if not isinstance(fv, tuple) or not len(fv) == 2:
            raise Exception(f"bad fv: {fv}")
        _patch({"name": fv[0], "value": fv[1]}, patch)
    if fvt is not None:
        if not isinstance(fvt, tuple) or not len(fvt) == 3:
            raise Exception(f"bad fv: {fvt}")
        _patch({"name": fvt[0], "value": fvt[1], "type": fvt[2]}, patch)
    return _patch(
        patch,
        dict(
            name="generic_feature",
            value="generic_value",
            type="string",
        ),
    )


def binary_event(
    patch: dict = None,
    *,
    model=True,
    eid=None,
    authornv=None,
    authoru=None,
    sourceit=None,
    spath=None,
    spathl=None,
    features=None,
    fvl=None,
    fvtl=None,
    datas=None,
    authorsec=None,
    sourcesec=None,
    sourcesettings=None,
    sourcerefs=None,
    timestamp=None,
    info=None,
    ssdeep=None,
    tlsh=None,
    post_patch=None,
    data_patch=None,
    action=None,
    magicmime=None,
    dp: bool = True,
    dequeued: str = None,
) -> dict | azm.BinaryEvent:
    """Assemble a generic binary event while allowing specific properties to overwrite.

    patch is overridden by any other specified option.
    post_patch overrides any other specified option.
    """
    if not patch:
        patch = {}

    # patch the patch
    if eid is not None:
        _patch(
            {
                "entity": {
                    "sha256": eid,
                    "sha512": f"{eid:0>128}",
                    "sha1": f"{eid[:40]:0>40}",
                    "md5": f"{eid[:32]:0>32}",
                    "datastreams": [data(patch=data_patch, hash=eid)],
                }
            },
            patch,
        )

    if authornv is not None:
        _patch({"author": {"name": authornv[0], "version": authornv[1]}}, patch)
    if authoru is not None:
        _patch({"author": {"category": "user", "name": authoru, "version": "1"}}, patch)
    if sourceit is not None:
        _patch({"source": {"name": sourceit[0], "timestamp": sourceit[1]}}, patch)
    if spathl is not None:
        spath = [path(eid=x[0], authornv=x[1]) for x in spathl]
    if spath is not None:
        _patch({"source": {"path": spath}}, patch)
        if len(spath) > 0:
            # if there is other stuff on the path, it can't be sourced
            _patch({"action": azm.BinaryAction.Extracted}, patch)
    if features is not None:
        _patch({"entity": {"features": features}}, patch)
    if action is not None:
        _patch({"action": action}, patch)
    if fvl is not None:
        _patch({"entity": {"features": [feature(fv=x) for x in fvl]}}, patch)
    if fvtl is not None:
        _patch({"entity": {"features": [feature(fvt=x) for x in fvtl]}}, patch)
    if datas is not None:
        _patch({"entity": {"datastreams": datas}}, patch)
    if authorsec is not None:
        _patch({"author": {"security": authorsec}}, patch)
    if sourcesec is not None:
        _patch({"source": {"security": sourcesec}}, patch)
    if sourcesettings is not None:
        _patch({"source": {"settings": sourcesettings}}, patch)
    if timestamp:
        _patch({"timestamp": timestamp}, patch)
    if info is not None:
        _patch({"entity": {"info": info}}, patch)
    if ssdeep:
        _patch({"entity": {"ssdeep": ssdeep}}, patch)
    if tlsh:
        _patch({"entity": {"tlsh": tlsh}}, patch)
    if dequeued:
        _patch({"dequeued": dequeued}, patch)
    if magicmime:
        _patch({"entity": {"magic": magicmime[0], "mime": magicmime[1]}}, patch)
    if post_patch is not None:
        _patch(post_patch, patch)

    # patch the base doc
    ret = _patch(
        patch,
        dict(
            model_version=azm.CURRENT_MODEL_VERSION,
            kafka_key="test-meta-tmp",
            dequeued="a dequeued id",
            retries=0,
            timestamp="2021-01-01T12:00:00+00:00",
            action=azm.BinaryAction.Sourced,
            author=dict(
                category="plugin",
                name="generic_plugin",
                version="2021-01-01T12:00:00+00:00",
                security=g1_1,
            ),
            source=dict(
                name="generic_source",
                timestamp="2021-01-01T11:00:00+00:00",
                references={"ref1": "val1", "ref2": "val2"},
                security=g1_1,
                path=[],
            ),
            entity=dict(
                size=1024,
                sha512=f"{'ab':0>128}",
                sha256=f"{'ab':0>64}",
                sha1=f"{'ab':0>40}",
                md5=f"{'ab':0>32}",
                ssdeep="1:1:1",
                tlsh="T1" + "0" * 70,
                mime="text/plain",
                magic="ASCII text",
                file_format_legacy="Text",
                file_format="text/plain",
                file_extension="txt",
                features=[feature()],
                datastreams=[data()],
                info={},
            ),
        ),
    )
    if sourcerefs is not None:
        ret["source"]["references"] = sourcerefs

    if action in [azm.BinaryAction.Extracted, azm.BinaryAction.Augmented, azm.BinaryAction.Enriched]:
        if len(ret["source"]["path"]) == 0:
            # attach generic parent or the event is technically invalid
            ret["source"]["path"].append(
                path(
                    {
                        "sha256": "default_parent",
                        "author": dict(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security=g1_1,
                        ),
                        "action": azm.BinaryAction.Sourced,
                        "timestamp": ret["timestamp"],
                    }
                )
            )

    # add current entity to source path
    ret["source"]["path"].append(
        path(
            {
                "sha256": ret["entity"]["sha256"],
                "author": copy.deepcopy(ret["author"]),
                "action": ret["action"],
                "timestamp": ret["timestamp"],
            }
        )
    )
    # check valid
    ret = azm.BinaryEvent(**ret)
    # add dispatcher enriched data (tracking + id)
    if dp:
        add_binary_tracking(ret)

    if ret.action in [azm.BinaryAction.Enriched, azm.BinaryAction.Mapped]:
        # cannot have entity data
        ret.entity.datastreams = []
    if not model:
        ret = basic_events.jsondict(ret)
    return ret


def plugin_feature(patch: dict = None, *, name=None):
    if not patch:
        patch = {}
    if name is not None:
        _patch({"name": name}, patch)
    return _patch(
        patch,
        dict(
            name="generic_feature",
            desc="generic_description",
            type="string",
        ),
    )


def plugin(
    patch: dict = None, *, model=True, authornv=None, features=None, config=None, authorsec=None, timestamp=None
) -> dict | azm.PluginEvent:
    if not patch:
        patch = {}
    if timestamp:
        _patch({"timestamp": timestamp}, patch)
    if authornv is not None:
        _patch({"author": {"name": authornv[0], "version": authornv[1]}}, patch)
        _patch({"entity": {"name": authornv[0], "version": authornv[1]}}, patch)

    if features is not None:
        _patch({"entity": {"features": [plugin_feature(name=x) for x in features]}}, patch)
    if config is not None:
        _patch({"entity": {"config": config}}, patch)
    if authorsec is not None:
        _patch({"author": {"security": authorsec}}, patch)
        _patch({"entity": {"security": authorsec}}, patch)
    ret = _patch(
        patch,
        dict(
            model_version=azm.CURRENT_MODEL_VERSION,
            kafka_key="test-meta-tmp",
            author=dict(
                category="plugin",
                name="generic_plugin",
                version="2021-01-01T12:00:00+00:00",
                security="LOW TLP:CLEAR",
            ),
            entity=dict(
                category="plugin",
                name="generic_plugin",
                version="2021-01-01T12:00:00+00:00",
                contact="generic_contact",
                description="generic_description",
                features=[plugin_feature(name="generic_feature")],
                security="LOW TLP:CLEAR",
                config={},
            ),
            timestamp="2000-01-01T01:01:01Z",
        ),
    )

    # check valid
    ret = azm.PluginEvent(**ret)
    ret.kafka_key = generate_event_id(plugin=ret)
    if not model:
        ret = basic_events.jsondict(ret)
    return ret


def entity_tag(patch=None, *, eid=None, tag=None):
    if not patch:
        patch = {}
    if eid is not None:
        _patch({"sha256": eid}, patch)
    if tag is not None:
        _patch({"tag": tag}, patch)
    return _patch(
        patch,
        dict(
            sha256="generic_binary",
            tag="default-tag",
            timestamp="2000-01-01T01:01:01Z",
            security=g1_1,
        ),
    )


def feature_value_tag(patch=None, *, fv=None, tag=None):
    if not patch:
        patch = {}
    if fv is not None:
        _patch({"feature_name": fv[0], "feature_value": fv[1]}, patch)
    if tag is not None:
        _patch({"tag": tag}, patch)
    return _patch(
        patch,
        dict(
            feature_name="generic_feature",
            feature_value="generic_value",
            tag="generic_tag",
            timestamp="2000-01-01T01:01:01Z",
            security=g1_1,
        ),
    )


def status(
    patch=None,
    *,
    model=True,
    eid=None,
    authornv=None,
    status=None,
    ts=None,
    authorsec=None,
    sourcesec=None,
    errorm=None,
    entid=None,
    timestamp=None,
    runtime=None,
) -> dict | azm.StatusEvent:
    if not patch:
        patch = {}
    if timestamp:
        _patch({"timestamp": timestamp}, patch)
    if authornv is not None:
        _patch({"author": {"name": authornv[0], "version": authornv[1]}}, patch)
    if status is not None:
        _patch({"entity": {"status": status}}, patch)
    if eid is not None:
        _patch({"entity": {"input": {"entity": {"sha256": eid}}}}, patch)
    if ts is not None:
        _patch({"timestamp": ts}, patch)
    if authorsec is not None:
        _patch({"author": {"security": authorsec}}, patch)
    if sourcesec is not None:
        _patch({"source": {"security": sourcesec}}, patch)
    if errorm:
        _patch({"entity": {"error": errorm[0], "message": errorm[1]}}, patch)
    if entid is not None:
        _patch({"entity": {"kafka_key": entid}}, patch)
    if runtime is not None:
        _patch({"entity": {"runtime": runtime}}, patch)

    ret = _patch(
        patch,
        dict(
            model_version=azm.CURRENT_MODEL_VERSION,
            kafka_key="test-meta-tmp",
            timestamp="2000-01-01T01:01:01Z",
            author=dict(
                category="plugin",
                name="generic_plugin",
                version="1",
                security=g1_1,
            ),
            entity=dict(
                status="heartbeat",
                runtime=10,
                input=binary_event(
                    model=False,
                    eid=eid or "test-meta-tmp",
                    authornv=("generic_plugin", "1"),
                    sourceit=("generic_source", "2000-01-01T01:01:01Z"),
                ),
            ),
        ),
    )

    # check valid
    ret = azm.StatusEvent(**ret)
    ret.entity.input.kafka_key = generate_event_id(binary=ret.entity.input)
    ret.entity.input.dequeued = ret.entity.input.kafka_key + "." + ret.author.name + "." + ret.author.version
    ret.kafka_key = generate_event_id(status=ret)
    if not model:
        ret = basic_events.jsondict(ret)
    return ret


def manual_insert(patch=None, *, authornv=None, dp: bool = True):
    if not patch:
        patch = {}
    if authornv is not None:
        _patch({"author": {"name": authornv[0], "version": authornv[1]}}, patch)

    ret = _patch(
        patch,
        {
            "model_version": azm.CURRENT_MODEL_VERSION,
            "kafka_key": "test-meta-tmp",
            "author": {
                "category": "user",
                "name": "user1",
                "security": g1_1,
            },
            "entity": {
                "original_source": "testing",
                "parent_sha256": "e1",
                "child": {
                    "datastreams": [data()],
                    "md5": "ab" * 16,
                    "sha1": "ab" * 20,
                    "sha256": "e1111",
                    "sha512": "ab" * 64,
                    "size": 121,
                    "file_format_legacy": "Text",
                    "features": [
                        {"name": "file_format_legacy", "type": "string", "value": "Text"},
                        {"name": "magic", "type": "string", "value": "ASCII text"},
                        {"name": "mime", "type": "string", "value": "text/plain"},
                    ],
                },
                "child_history": {
                    "author": {
                        "category": "user",
                        "name": "user1",
                        "security": g1_1,
                    },
                    "action": azm.BinaryAction.Sourced,
                    "sha256": "e1111",
                    "filename": "test.txt",
                    "size": 121,
                    "file_format_legacy": "Text",
                    "relationship": {},
                    "timestamp": "2020-06-02T11:47:03.200000Z",
                },
            },
            "timestamp": "2021-03-30T21:44:50.703063Z",
        },
    )

    # check valid
    ret = azm.InsertEvent(**ret)
    ret.kafka_key = generate_event_id(insert=ret)
    if dp:
        add_insert_tracking(ret)
    ret = basic_events.jsondict(ret)
    return ret


def add_binary_tracking(ev: azm.BinaryEvent):
    """Logic duped (approximately) from dispatcher."""
    ev.kafka_key = generate_event_id(binary=ev)
    ev.track_source_references = f"{ev.source.name}."
    vals = ""
    for k in sorted(ev.source.references.keys()):
        vals += k + "." + ev.source.references[k] + "."
    ev.track_source_references += md5(vals)

    ev.track_authors = []
    ev.track_links = []
    parent = -1
    for i, node in enumerate(ev.source.path):
        author = f"{node.author.category}.{node.author.name}.{node.author.version}"
        ev.track_authors.append(author)
        if parent >= 0:
            parentid = ev.source.path[parent].sha256
            ev.track_links.append(f"{parentid}.{node.sha256}.{author}")
        parent = i
    return ev


def add_insert_tracking(ev: azm.InsertEvent):
    """Logic duped (approximately) from dispatcher."""
    ev.kafka_key = generate_event_id(insert=ev)
    node = ev.entity.child_history
    author = f"{node.author.category}.{node.author.name}.{node.author.version}"
    ev.track_author = author
    ev.track_link = f"{ev.entity.parent_sha256}.{node.sha256}.{author}"
    return ev


def generate_event_id(
    *,
    plugin: azm.PluginEvent = None,
    status: azm.StatusEvent = None,
    insert: azm.InsertEvent = None,
    binary: azm.BinaryEvent = None,
) -> str:
    """Calculate and return a document id for the event.

    For manual insert events, we need to generate a bunch of docs directly to opensearch.

    This should be very similar to that generated by dispatcher.
    """
    s = ""
    if plugin:
        return f"meta.{ plugin.entity.name }.{plugin.entity.version}"
    elif status:
        return f"meta.{status.entity.input.dequeued}.{status.author.name}"
    elif insert:
        return f"meta.{insert.entity.parent_sha256}.{insert.entity.child.sha256}.{insert.entity.child_history.author.name}"
    elif binary:
        s += binary.source.name + "."
        s += binary.source.security + "."
        s += binary.source.timestamp.isoformat() + "."
        for k, v in binary.source.references.items():
            s += k + "." + v + "."
        first = None
        parent = None
        current = None
        securities = []
        for y in binary.source.path:
            if not first:
                first = y
            if current:
                parent = current
            current = y
            securities.append(y.author.security)

        securities = sorted(set(securities))
        for sec in securities:
            s += sec + "."

        if not current:
            raise Exception("bad path, no items found")

        for y in [first, parent, current]:
            if not y:
                continue
            s += y.author.name + "."
            s += y.action + "."
            s += y.sha256 + "."
    else:
        # could do a md5 of json dumped data here
        raise Exception("no id generation")
    # specifically mark as generated by metastore
    return "meta." + md5(s)
