from sweet.registry import installed_packages, is_installed


def test_dummy_package_is_discovered_when_installed() -> None:
    # sweet-dummy is a workspace dev dependency, always present when synced
    # (unlike sweet-etl, which is an optional client-facing extra).
    installed_packages.cache_clear()

    names = {package.name for package in installed_packages()}

    assert "dummy" in names
    assert is_installed("dummy")


def test_unknown_package_is_not_installed() -> None:
    installed_packages.cache_clear()

    assert not is_installed("does-not-exist")
