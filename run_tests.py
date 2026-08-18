"""Test runner simple que ejecuta todas las pruebas unitarias y de integración del proyecto."""
import glob
import importlib
import inspect
import sys


def main() -> int:
    total, passed, failed = 0, 0, 0
    test_files = sorted(glob.glob("tests/test_*.py"))

    print(f"=== Ejecutando suite de pruebas CivicMesh ({len(test_files)} archivos) ===\n")

    for f in test_files:
        mod_name = f.replace("/", ".").replace("\\", ".")[:-3]
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            print(f"[ERROR] No se pudo importar {mod_name}: {e}")
            return 1

        for name, func in inspect.getmembers(mod, inspect.isfunction):
            if name.startswith("test_"):
                total += 1
                try:
                    func()
                    passed += 1
                    print(f"  [PASS] {mod_name}.{name}")
                except Exception as e:
                    failed += 1
                    print(f"  [FAIL] {mod_name}.{name}: {e}")

    print(f"\nResumen: {passed}/{total} pruebas superadas, {failed} fallidas.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
