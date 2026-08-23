from domains.crime.generator import CrimeGenerator


def test_crime_generator_is_reproducible_with_same_seed():
    rates = {"Santiago": {"robo": 0.9, "hurto": 0.6}}
    g1 = CrimeGenerator(rates, delta_t=1.0, seed=42)
    g2 = CrimeGenerator(rates, delta_t=1.0, seed=42)

    seq1 = [g1.generate("Santiago", t) for t in range(20)]
    seq2 = [g2.generate("Santiago", t) for t in range(20)]
    assert seq1 == seq2


def test_crime_generator_outputs_non_negative_counts():
    rates = {"Santiago": {"robo": 1.2}}
    generator = CrimeGenerator(rates, delta_t=1.0, seed=7)
    assert all(
        event.count >= 0
        for t in range(20)
        for event in generator.generate("Santiago", t)
    )
