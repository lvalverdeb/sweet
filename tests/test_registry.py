from boti_sweet.registry import installed_packages, is_installed


def test_etl_package_is_discovered_when_installed() -> None:
    installed_packages.cache_clear()

    names = {package.name for package in installed_packages()}

    assert "etl" in names
    assert is_installed("etl")


def test_unknown_package_is_not_installed() -> None:
    installed_packages.cache_clear()

    assert not is_installed("does-not-exist")
