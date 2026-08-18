from network.topology import GeoTopology, DEFAULT_COMMUNE_ADJACENCY


def test_topology_neighbors():
    topo = GeoTopology()
    stgo_neighbors = topo.get_neighbors("Santiago")
    assert "Providencia" in stgo_neighbors
    assert "Recoleta" in stgo_neighbors
    assert "Estación Central" in stgo_neighbors


def test_topology_symmetry():
    topo = GeoTopology()
    assert topo.are_neighbors("Santiago", "Providencia")
    assert topo.are_neighbors("Providencia", "Santiago")
    assert topo.are_neighbors("Santiago", "Santiago")


def test_topology_distance_hops():
    topo = GeoTopology()
    assert topo.distance_hops("Santiago", "Santiago") == 0
    assert topo.distance_hops("Santiago", "Providencia") == 1
    # Santiago -> Providencia -> Las Condes (2 hops)
    assert topo.distance_hops("Santiago", "Las Condes") == 2
    # Santiago -> Providencia -> Las Condes -> Lo Barnechea (3 hops)
    assert topo.distance_hops("Santiago", "Lo Barnechea") == 3


def test_topology_custom_graph():
    custom = {
        "A": ["B"],
        "B": ["C"],
        "C": ["D"],
    }
    topo = GeoTopology(custom)
    assert topo.distance_hops("A", "D") == 3
    assert topo.get_neighbors("B") == ["A", "C"]
    assert topo.distance_hops("A", "NonExistent") == 999
