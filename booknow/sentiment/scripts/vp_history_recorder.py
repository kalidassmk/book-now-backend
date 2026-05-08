"""Supervisor entrypoint for the vp_history recorder subsystem.

Lives under sentiment/scripts/ to fit the supervisor's flat cwd layout.
The actual recorder is in ``booknow.subsystems.vp_history.recorder``.
"""

from booknow.subsystems.vp_history.recorder import main


if __name__ == "__main__":
    main()
