"""Supervisor entrypoint shim for the stale_cleaner subsystem."""

from booknow.subsystems.stale_cleaner.cleaner import main


if __name__ == "__main__":
    main()
