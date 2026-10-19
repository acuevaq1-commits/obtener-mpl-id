# Monitor de MPL-ID

Consulta registros con `status=ERROR` en `iislog.iis_run`, obtiene el detalle
del MPL-ID desde SAP Integration Suite y lo guarda en `iislog.iis_mpl_id`.
No reenvía mensajes ni vuelve a ejecutar integraciones.

## Preparación en Windows

Requiere Python 3.11 o posterior, acceso a PostgreSQL y SAP Integration Suite.

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m playwright install chromium
.\.venv\Scripts\python.exe consultar_mpl.py --init-env
```

Completa `.env.postgresql` con los valores de tu entorno. Este archivo contiene
credenciales y está excluido de Git. No copies `.venv` al servidor: créalo allí
con los mismos comandos. Instala Chromium bajo la cuenta que ejecutará la tarea.

## Uso

```powershell
.\.venv\Scripts\python.exe consultar_mpl.py --self-test
.\.venv\Scripts\python.exe consultar_mpl.py
.\.venv\Scripts\python.exe consultar_mpl.py --fecha 19/10/2026
.\.venv\Scripts\python.exe consultar_mpl_hoy.py
```

`consultar_mpl.py` solicita la fecha cuando se ejecuta sin `--fecha`.
`consultar_mpl_hoy.py` usa la fecha actual según `DATE_TIMEZONE` y sirve como
entrada para el Programador de tareas. Los logs se guardan en `LOG_DIR`.

Para una tarea cada hora, configura el programa como la ruta absoluta de
`.venv\Scripts\python.exe`, el argumento como la ruta absoluta de
`consultar_mpl_hoy.py` y la opción «No iniciar una nueva instancia».
El modo automático requiere que el inicio de sesión SAP funcione sin
intervención. Los registros de días anteriores se consultan manualmente con
`--fecha`.

