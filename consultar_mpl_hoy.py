#!/usr/bin/env python3
"""Procesa los registros del día actual sin solicitar una fecha.

Pensado para ejecutarse cada hora desde el Programador de tareas. Usa la
zona horaria DATE_TIMEZONE de .env.postgresql (America/Lima por defecto).
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def main():
    project_dir = Path(__file__).resolve().parent
    env_file = project_dir / ".env.postgresql"
    load_dotenv(env_file, override=False, interpolate=False)

    zone = ZoneInfo(os.environ.get("DATE_TIMEZONE", "America/Lima"))
    fecha = datetime.now(zone).strftime("%d/%m/%Y")

    command = [
        sys.executable,
        str(project_dir / "consultar_mpl.py"),
        "--env",
        str(env_file),
        "--fecha",
        fecha,
    ]
    return subprocess.run(command, cwd=project_dir, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())

