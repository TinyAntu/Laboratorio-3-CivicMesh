from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional


# Topología geográfica y adyacencia de comunas (ej. Gran Santiago)
DEFAULT_COMMUNE_ADJACENCY: dict[str, list[str]] = {
    "Santiago": ["Estación Central", "Recoleta", "Independencia", "Providencia", "Ñuñoa", "San Miguel", "Quinta Normal"],
    "Providencia": ["Santiago", "Recoleta", "Las Condes", "Ñuñoa", "Vitacura"],
    "Las Condes": ["Providencia", "Vitacura", "Lo Barnechea", "La Reina", "Peñalolén"],
    "Vitacura": ["Providencia", "Las Condes", "Lo Barnechea", "Huechuraba", "Recoleta"],
    "Lo Barnechea": ["Las Condes", "Vitacura"],
    "Ñuñoa": ["Santiago", "Providencia", "La Reina", "Peñalolén", "Macul", "San Joaquín"],
    "La Reina": ["Las Condes", "Ñuñoa", "Peñalolén"],
    "Peñalolén": ["Las Condes", "La Reina", "Ñuñoa", "Macul", "La Florida"],
    "Macul": ["Ñuñoa", "Peñalolén", "San Joaquín", "La Florida"],
    "La Florida": ["Peñalolén", "Macul", "San Joaquín", "La Granja", "Puente Alto"],
    "Puente Alto": ["La Florida", "La Pintana", "Pirque", "San José de Maipo"],
    "San Joaquín": ["Santiago", "San Miguel", "Ñuñoa", "Macul", "La Florida", "La Granja"],
    "San Miguel": ["Santiago", "San Joaquín", "Pedro Aguirre Cerda", "La Cisterna", "San Ramón"],
    "Estación Central": ["Santiago", "Quinta Normal", "Lo Prado", "Maipú", "Cerrillos"],
    "Maipú": ["Estación Central", "Cerrillos", "Pudahuel", "Padre Hurtado"],
    "Pudahuel": ["Maipú", "Lo Prado", "Cerro Navia", "Quilicura", "Renca"],
    "Quilicura": ["Pudahuel", "Renca", "Conchalí", "Huechuraba", "Lampa"],
    "Recoleta": ["Santiago", "Independencia", "Conchalí", "Huechuraba", "Providencia", "Vitacura"],
    "Independencia": ["Santiago", "Recoleta", "Conchalí", "Renca", "Quinta Normal"],
    "Quinta Normal": ["Santiago", "Independencia", "Renca", "Cerro Navia", "Lo Prado", "Estación Central"],
    "Renca": ["Quinta Normal", "Independencia", "Conchalí", "Quilicura", "Pudahuel", "Cerro Navia"],
    "Lo Prado": ["Quinta Normal", "Cerro Navia", "Pudahuel", "Estación Central"],
    "Cerro Navia": ["Quinta Normal", "Renca", "Pudahuel", "Lo Prado"],
    "Conchalí": ["Independencia", "Recoleta", "Huechuraba", "Quilicura", "Renca"],
    "Huechuraba": ["Conchalí", "Recoleta", "Vitacura", "Quilicura"],
    "Cerrillos": ["Estación Central", "Maipú", "Pedro Aguirre Cerda", "San Bernardo"],
    "Pedro Aguirre Cerda": ["Estación Central", "Santiago", "San Miguel", "Cerrillos", "Lo Espejo"],
    "Lo Espejo": ["Pedro Aguirre Cerda", "San Miguel", "La Cisterna", "Cerrillos", "San Bernardo"],
    "La Cisterna": ["San Miguel", "San Ramón", "El Bosque", "Lo Espejo"],
    "San Ramón": ["San Miguel", "San Joaquín", "La Granja", "La Cisterna", "La Pintana"],
    "La Granja": ["San Joaquín", "La Florida", "San Ramón", "La Pintana"],
    "La Pintana": ["La Granja", "La Florida", "Puente Alto", "San Ramón", "El Bosque", "San Bernardo"],
    "El Bosque": ["La Cisterna", "San Ramón", "La Pintana", "San Bernardo"],
    "San Bernardo": ["El Bosque", "La Pintana", "Lo Espejo", "Cerrillos", "Calera de Tango", "Buin"],
}


class GeoTopology:
    """Gestiona la topología espacial de comunas, adyacencias y cálculo de distancias por saltos."""

    def __init__(self, adjacency: dict[str, list[str]] | None = None):
        self.adjacency: dict[str, set[str]] = {}
        data = adjacency if adjacency is not None else DEFAULT_COMMUNE_ADJACENCY
        for node, neighbors in data.items():
            if node not in self.adjacency:
                self.adjacency[node] = set()
            for neighbor in neighbors:
                self.adjacency[node].add(neighbor)
                if neighbor not in self.adjacency:
                    self.adjacency[neighbor] = set()
                self.adjacency[neighbor].add(node)

    def get_neighbors(self, commune: str) -> list[str]:
        """Retorna las comunas adyacentes directas a la comuna dada."""
        return sorted(list(self.adjacency.get(commune, set())))

    def are_neighbors(self, c1: str, c2: str) -> bool:
        """Verifica si dos comunas son adyacentes directas."""
        if c1 == c2:
            return True
        return c2 in self.adjacency.get(c1, set())

    def distance_hops(self, c1: str, c2: str) -> int:
        """Calcula la distancia mínima en saltos (BFS) entre dos comunas en el grafo."""
        if c1 == c2:
            return 0
        if c1 not in self.adjacency or c2 not in self.adjacency:
            return 999  # No alcanzable o fuera del grafo

        queue: list[tuple[str, int]] = [(c1, 0)]
        visited: set[str] = {c1}

        while queue:
            current, dist = queue.pop(0)
            for neighbor in self.adjacency.get(current, set()):
                if neighbor == c2:
                    return dist + 1
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, dist + 1))

        return 999

    def all_communes(self) -> list[str]:
        """Retorna la lista de todas las comunas registradas en la topología."""
        return sorted(list(self.adjacency.keys()))
