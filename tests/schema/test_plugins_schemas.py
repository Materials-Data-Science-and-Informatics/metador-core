import pytest
from pydantic import ValidationError

from metador_core.schema.plugins import PluginBase, PluginRef


def test_pluginref():
    with pytest.raises(ValidationError):
        # extra fields forbidden
        PluginRef(group="a", name="b", version=(0, 1, 0), xtra="haha")

    ref = PluginRef(group="a", name="b", version=(0, 1, 0))
    ref2 = PluginRef(group="mygroup", name="b", version=(0, 1, 0))

    with pytest.raises(TypeError):
        ref.group = "x"  # immutable

    assert len(str(ref).split("\n")) == 1  # str without indentation (wasteful)

    # test subclass
    MyPluginRef = PluginRef._subclass_for("mygroup")
    mref = MyPluginRef(name="b", version=(0, 1, 0))

    with pytest.raises(ValidationError):
        # cannot override group for subclass
        MyPluginRef(**ref.dict())

    # hash works as expected (class does not matter, only by value)
    s = set()
    s.add(mref)
    assert mref in s
    assert ref2 in s

    # (in)equality also works between subclasses
    assert mref != ref
    assert mref == MyPluginRef(name="b", version=(0, 1, 0))
    assert mref == ref2

    # comparison works
    assert ref < PluginRef(group="b", name="a", version=(1, 0, 0))  # first by group
    assert mref < MyPluginRef(name="c", version=(0, 0, 1))  # by name
    assert mref < MyPluginRef(name="b", version=(0, 1, 1))  # by version

    # test "supports"
    assert not ref.supports(ref2)  # other group
    assert not ref.supports(ref.copy(update=dict(name="y")))  # other name
    assert ref.supports(ref.copy(update=dict(version=(0, 0, 1))))
    assert ref.supports(ref.copy(update=dict(version=(0, 1, 1))))
    assert not ref.supports(ref.copy(update=dict(version=(0, 2, 0))))
    assert not ref.supports(ref.copy(update=dict(version=(1, 0, 0))))


def test_pluginbase(plugingroups_test):
    class DummyPlugin(PluginBase):
        ...

    DummyPlugin.group = "schema"

    # test invalid
    with pytest.raises(TypeError):

        class DummyInfoFail1:
            name = "dummy"
            version = "1.2.3"  # <- invalid type

        DummyPlugin.parse_info(DummyInfoFail1)

    with pytest.raises(ValueError):
        # mismatch with EP name
        class DummyInfoFail2:
            name = "dummy"
            version = (1, 2, 3)

        DummyPlugin.parse_info(DummyInfoFail2, ep_name="dummy__4.5.6")

    # test valid
    class DummyInfo:
        name = "dummy"
        version = (1, 2, 3)

    info = DummyPlugin.parse_info(DummyInfo, ep_name="dummy__1.2.3")
    info = DummyPlugin.parse_info(DummyInfo)
    assert info.ref() == PluginRef(group="schema", name="dummy", version=(1, 2, 3))
    assert info.ref(version=(4, 5, 6)) == PluginRef(
        group="schema", name="dummy", version=(4, 5, 6)
    )

    assert info.plugin_string() == "metador.schema.dummy.1.2.3"

    # pretty printed JSON str
    assert len(str(info).split("\n")) > 1
