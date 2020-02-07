import pytest

from bionic.descriptors import DescriptorNode, EntityNode


@pytest.mark.parametrize('descriptor', ['data', 'data2', 'data_data', '__1'])
def test_valid_descriptor_can_be_parsed_and_unparsed(descriptor):
    dnode = DescriptorNode.from_descriptor(descriptor)
    assert dnode == EntityNode(descriptor)
    assert dnode.to_descriptor() == descriptor


@pytest.mark.parametrize('descriptor', ['', 'x x', 'x.x', '(x)', '1x'])
def test_invalid_descriptor_throws_exception(descriptor):
    with pytest.raises(ValueError):
        DescriptorNode.from_descriptor(descriptor)


def test_descriptor_to_entity_name():
    assert EntityNode('x').to_entity_name() == 'x'


def test_dnode_equality():
    assert EntityNode('x') == EntityNode('x')
    assert EntityNode('x') != EntityNode('y')


def test_dnode_hashing():
    names_by_dnode = {
        EntityNode('x'): 'x',
        EntityNode('y'): 'y',
    }
    assert names_by_dnode[EntityNode('x')] == 'x'
    assert names_by_dnode[EntityNode('y')] == 'y'
