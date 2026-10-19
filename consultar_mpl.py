#!/usr/bin/env python3
"""PostgreSQL -> SAP Integration Suite -> PostgreSQL. Python 3.11+.

Instalar:
  python -m pip install "psycopg[binary]" playwright python-dotenv tzdata
  python -m playwright install chromium
Configurar y ejecutar:
  python consultar_mpl.py --init-env
  # Editar .env.postgresql (conexiones, contraseña SAP, tabla y ruta JSON).
  python consultar_mpl.py  # solicita la fecha en DD/MM/YYYY
  python consultar_mpl.py --fecha 09/10/2026
  python consultar_mpl.py --mpl-id AGrH3H1b_fk4Eo4YHU1MuZZ3R8Gn
  python consultar_mpl.py --self-test

--mpl-id consulta SAP y muestra el mensaje sin acceder a las bases de datos.
Origen: PostgreSQL, iislog.iis_run (ts, run_id, summary_json).
Tres hilos independientes procesan pi_id 00021, 00022 (cargos) y 00023 (pagos)
con status ERROR; cada uno usa su propia conexión PostgreSQL y sesión SAP.
La fecha se solicita en DD/MM/YYYY; cada ejecución guarda un log en LOG_DIR.
El MPL-ID se extrae también del texto en summary_json.failed_items[].body.
Si no existe el adjunto HTTP_Receiver_Adapter_Response_Body, texto_legible
guarda 'Revisar log de CPI' y texto_cpi guarda la respuesta completa de
MessageProcessingLogs('<MPL-ID>')/ErrorInformation/$value.
Destino: PostgreSQL, iislog.iis_mpl_id en la misma base de datos y conexión.
Se conservan el ts y run_id originales para respetar la clave foránea.
No se reemplaza ts por now() y no se crean padres artificiales.
texto_mpl_id guarda el MPL-ID extraído, no todo summary_json.
consultado=N significa pendiente o fallido; S significa mensaje obtenido y guardado.
Los pendientes se reintentan y los S se omiten. Cada registro se confirma por separado.
No se persisten cookies ni contraseña en el código; sesión nueva en cada ejecución.
El login parte de /shell/home para generar state/nonce/PKCE vigentes.
No utilizar la URL oauth2/authorize copiada de una sesión anterior.
La automatización del formulario depende del tenant: selectores configurables.
Si hay MFA, ejecutar con SAP_HEADLESS=false y completar el paso en la ventana.
No se desactiva la validación TLS. No imprime credenciales ni cuerpos HTML.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote, unquote, urljoin, urlsplit
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

LOG = logging.getLogger("mpl")
PI_IDS = ("00021", "00022", "00023")
REVIEW_CPI_MESSAGE = "Revisar log de CPI"
ATTACHMENT_NAME = "HTTP_Receiver_Adapter_Response_Body"
ENV_TEMPLATE = '''# Completar antes de ejecutar. No subir este archivo a Git.
# Fecha del filtro interpretada en esta zona horaria (ts es TIMESTAMPTZ).
DATE_TIMEZONE=America/Lima
LOG_DIR=logs
# Una sola conexión para leer iislog.iis_run y escribir iislog.iis_mpl_id.
PG_HOST=
PG_PORT=5432
PG_DATABASE=
PG_USER=
PG_PASSWORD=
PG_SSLMODE=prefer
PG_FETCH_SIZE=200
SAP_BASE_URL=
SAP_IDP_HOST=
SAP_USERNAME=
SAP_PASSWORD=
SAP_HEADLESS=true
SAP_LOGIN_TIMEOUT_SECONDS=180
SAP_USERNAME_SELECTOR=#j_username
SAP_PASSWORD_SELECTOR=#j_password
SAP_SUBMIT_SELECTOR=.fn-button__text
HTTP_TIMEOUT_SECONDS=60
HTTP_ATTEMPTS=3
# Ruta JSON exacta si la conoces; formato JSON Pointer.
# Ejemplo para {"meta":{"mpl_id":"..."}}: /meta/mpl_id
# Vacía: busca claves MPL-ID, mpl_id, MPLID, MessageProcessingLogID
# y enlaces de monitor con identifier. Ambigüedad => error, no elige al azar.
MPL_JSON_POINTER=
'''


class ProcessingError(Exception):
    pass


class AuthenticationError(ProcessingError):
    pass


class SapMessage(NamedTuple):
    texto_legible: str
    texto_cpi: str


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise ProcessingError(f"Falta configurar {name} en .env")
    return value


def read_json(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8-sig")
    for _ in range(3):
        if not isinstance(value, str):
            break
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            break
    return value


def validate_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{10,200}", value):
        raise ProcessingError("MPL-ID inválido; revisa MPL_JSON_POINTER")
    return value


def extract_mpl(value, pointer=""):
    obj = read_json(value)
    if pointer:
        if not pointer.startswith("/"):
            raise ProcessingError("MPL_JSON_POINTER debe iniciar con /")
        try:
            for part in pointer[1:].split("/"):
                part = part.replace("~1", "/").replace("~0", "~")
                obj = read_json(obj)
                obj = obj[int(part)] if isinstance(obj, list) else obj[part]
            return validate_id(obj)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ProcessingError("Ruta MPL_JSON_POINTER no encontrada") from exc
    found = set()

    def walk(node):
        node = read_json(node)
        if isinstance(node, dict):
            for key, item in node.items():
                normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
                if normalized in {"mplid", "messageprocessinglogid", "sapmessageprocessinglogid"}:
                    found.add(validate_id(item))
                walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            decoded = unquote(node)
            patterns = [r"MessageProcessingLogs\('([A-Za-z0-9_-]+)'\)",
                        r'"identifier"\s*:\s*"([A-Za-z0-9_-]+)"',
                        r'(?i)The MPL ID for the failed message is\s*:\s*([A-Za-z0-9_-]{10,200})',
                        r'(?i)MPL[-_ ]?ID\s*[:=]\s*[\"\']?([A-Za-z0-9_-]{10,200})']
            for pattern in patterns:
                found.update(validate_id(x) for x in re.findall(pattern, decoded))

    walk(obj)
    if len(found) != 1:
        raise ProcessingError(f"Se encontraron {len(found)} MPL-ID; configura la ruta exacta MPL_JSON_POINTER")
    return found.pop()


def local_tag(element):
    return element.tag.rsplit("}", 1)[-1]


def parse_attachments(body):
    """Devuelve propiedades por adjunto y enlace de la siguiente página."""
    if body.lstrip().startswith(("{", "[")):
        obj = json.loads(body)
        data = obj.get("d", obj) if isinstance(obj, dict) else obj
        if isinstance(data, list):
            return data, None
        return data.get("results", data.get("value", [])), data.get("__next") or obj.get("@odata.nextLink")
    root = ET.fromstring(body)
    entries = [root] if local_tag(root) == "entry" else [x for x in root if local_tag(x) == "entry"]
    items = []
    for entry in entries:
        for props in entry.iter():
            if local_tag(props) == "properties":
                items.append({local_tag(x): x.text or "" for x in props})
    next_url = next((x.attrib.get("href") for x in root
                     if local_tag(x) == "link" and x.attrib.get("rel") == "next"), None)
    return items, next_url


def extract_message(body):
    obj = read_json(body)
    if not isinstance(obj, dict):
        raise ProcessingError("El adjunto no contiene un objeto JSON de error")
    error = obj.get("error", obj)
    message = error.get("message") if isinstance(error, dict) else None
    if isinstance(message, dict):
        message = message.get("value")
    if not isinstance(message, str) or not message.strip():
        raise ProcessingError("No existe error.message.value ni message de texto en el JSON")
    return message.strip()


def extract_error_information(body):
    if not body.strip().lstrip("\ufeff").strip():
        raise ProcessingError("ErrorInformation/$value no contiene texto de error")
    return body


class SapSession:
    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self.base = required("SAP_BASE_URL").rstrip("/")
        base_url = urlsplit(self.base)
        if base_url.scheme != "https" or not base_url.netloc or base_url.path or base_url.query or base_url.fragment:
            raise ProcessingError("SAP_BASE_URL debe ser el origen HTTPS de Integration Suite, sin ruta ni parámetros")
        self.origin = (base_url.scheme, base_url.netloc)
        username, password = required("SAP_USERNAME"), required("SAP_PASSWORD")
        self.timeout = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "60")) * 1000
        self.pw = sync_playwright().start()
        try:
            self.browser = self.pw.chromium.launch(headless=os.environ.get("SAP_HEADLESS", "true").lower() == "true")
            self.context = self.browser.new_context()
            self.page = self.context.new_page()
            self.login(username, password)
        except Exception:
            self.pw.stop()
            raise
        return self

    def __exit__(self, *_):
        self.context.close()
        self.browser.close()
        self.pw.stop()

    def login(self, username, password):
        self.page.goto(self.base + "/shell/home", wait_until="domcontentloaded", timeout=self.timeout)
        deadline = time.monotonic() + int(os.environ.get("SAP_LOGIN_TIMEOUT_SECONDS", "180"))
        user_filled = password_filled = False
        username_submitted = final_submitted = idp_selected = False
        while time.monotonic() < deadline:
            current = urlsplit(self.page.url)
            # Solo rellenar credenciales en SAP Identity Authentication.
            if current.scheme == "https" and (current.hostname or "").endswith(".accounts.ondemand.com"):
                user = self.page.locator(os.environ.get("SAP_USERNAME_SELECTOR", "#j_username")).first
                pwd = self.page.locator(os.environ.get("SAP_PASSWORD_SELECTOR", "#j_password")).first
                submit = self.page.locator(os.environ.get("SAP_SUBMIT_SELECTOR", ".fn-button__text")).first
                if not user_filled and user.is_visible():
                    user.fill(username)
                    user_filled = True
                if not password_filled and pwd.is_visible():
                    pwd.fill(password)
                    password_filled = True
                if password_filled and not final_submitted and submit.is_visible():
                    submit.click()
                    final_submitted = True
                elif user_filled and not password_filled and not username_submitted and submit.is_visible():
                    submit.click()
                    username_submitted = True
            elif (current.scheme, current.netloc) == self.origin and current.path.startswith("/shell"):
                # Una página /shell no prueba autorización; comprobar la API.
                resp = self.context.request.get(self.base + "/api/v1/MessageProcessingLogs?$top=1",
                                                timeout=self.timeout, max_redirects=0)
                try:
                    if resp.status == 200 and "<html" not in resp.text()[:500].lower():
                        LOG.info("Sesión SAP verificada")
                        return
                    if resp.status == 403:
                        raise AuthenticationError("SAP HTTP 403: revisar permisos de monitorización y adjuntos")
                finally:
                    resp.dispose()
            elif ".authentication." in (current.hostname or "") and current.path == "/login":
                alert = self.page.locator("[role='alert'], .alert").first
                if alert.is_visible() and "Unable to verify user session" in alert.inner_text():
                    raise AuthenticationError("SAP no pudo verificar la sesión después del inicio de sesión")
                if not idp_selected:
                    idp_host = required("SAP_IDP_HOST")
                    for link in self.page.locator("a[href]").all():
                        target = urlsplit(urljoin(self.page.url, link.get_attribute("href")))
                        if target.scheme == "https" and target.hostname == idp_host and target.path == "/oauth2/authorize":
                            link.click()
                            idp_selected = True
                            break
            self.page.wait_for_timeout(500)
        raise AuthenticationError("Login no completado; revisa credenciales/selectores o completa MFA en el navegador")

    def get(self, url):
        if (urlsplit(url).scheme, urlsplit(url).netloc) != self.origin:
            raise ProcessingError("Enlace de paginación fuera del origen SAP configurado")
        attempts = int(os.environ.get("HTTP_ATTEMPTS", "3"))
        for attempt in range(attempts):
            try:
                response = self.context.request.get(url, timeout=self.timeout, max_redirects=0)
            except Exception as exc:
                if attempt + 1 == attempts:
                    raise ProcessingError("Consulta SAP agotó reintentos de red/timeout") from exc
                time.sleep(min(2 ** attempt, 8))
                continue
            try:
                status, body = response.status, response.text()
            finally:
                response.dispose()
            if status in {401, 403} or 300 <= status < 400 or "<html" in body[:500].lower():
                raise AuthenticationError(f"SAP HTTP {status}: sesión vencida, redirección o permiso insuficiente")
            if status == 429 or status >= 500:
                if attempt + 1 < attempts:
                    time.sleep(min(2 ** attempt, 8))
                    continue
            if status != 200:
                raise ProcessingError(f"Consulta SAP devolvió HTTP {status}")
            return body
        raise ProcessingError("HTTP_ATTEMPTS debe ser mayor que cero")

    def message(self, mpl):
        mpl = validate_id(mpl)
        log_url = self.base + "/api/v1/MessageProcessingLogs('" + quote(mpl, safe="") + "')"
        url = log_url + "/Attachments"
        matches, visited = [], set()
        while url:
            if url in visited or len(visited) >= 1000:
                raise ProcessingError("Paginación circular o excesiva en SAP")
            visited.add(url)
            items, next_url = parse_attachments(self.get(url))
            matches.extend(x for x in items if x.get("Name") == ATTACHMENT_NAME)
            url = urljoin(url, next_url) if next_url else None
        if not matches:
            body = self.get(log_url + "/ErrorInformation/$value")
            return SapMessage(REVIEW_CPI_MESSAGE, extract_error_information(body))
        ids = {str(x["Id"]) for x in matches if x.get("Id")}
        if len(ids) != 1:
            raise ProcessingError(f"Adjunto {ATTACHMENT_NAME}: {len(ids)} Id distintos; no se puede elegir uno inequívocamente")
        attachment_id = quote(ids.pop().replace("'", "''"), safe="")
        body = self.get(self.base + "/api/v1/MessageProcessingLogAttachments('" + attachment_id + "')/$value")
        message = extract_message(body)
        return SapMessage(message, message)


def source_rows(pg, inicio, fin, batch_size, pi_id, limit=None):
    """Lectura por lotes en el orden solicitado, sin cargar toda la fecha en RAM.

    Cada lote se materializa antes de insertar/actualizar en la misma conexión.
    No usa OFFSET ni mantiene una transacción abierta durante las consultas SAP.
    """
    last = None
    yielded = 0
    while limit is None or yielded < limit:
        count = batch_size if limit is None else min(batch_size, limit - yielded)
        params = [inicio, fin, pi_id, "ERROR"]
        sql = ("SELECT r.ts, r.run_id, r.summary_json "
               "FROM iislog.iis_run AS r "
               "WHERE r.ts >= %s AND r.ts < %s "
               "AND r.summary_json ->> 'pi_id' = %s AND r.status = %s")
        if last is not None:
            sql += " AND (r.ts < %s OR (r.ts = %s AND r.run_id > %s))"
            params.extend((last[0], last[0], last[1]))
        sql += " ORDER BY r.ts DESC, r.run_id ASC LIMIT %s"
        params.append(count)
        with pg.cursor() as cur:
            rows = cur.execute(sql, params).fetchall()
        if not rows:
            return
        for row in rows:
            yield row
            yielded += 1
        last = rows[-1][:2]


def run_pi(pi_id, inicio, fin, batch_size, limit):
    import psycopg
    LOG.info("pi_id=%s: iniciando registros con status=ERROR", pi_id)
    counters = dict(leidos=0, guardados=0, omitidos=0, fallidos=0)
    # Cada hilo conserva su propia conexión y sesión SAP; no comparte cursores,
    # contexto de Playwright ni estado de paginación con los demás.
    with psycopg.connect(
            host=required("PG_HOST"), port=int(os.environ.get("PG_PORT", "5432")),
            dbname=required("PG_DATABASE"), user=required("PG_USER"), password=required("PG_PASSWORD"),
            sslmode=os.environ.get("PG_SSLMODE", "prefer"), connect_timeout=20, autocommit=True) as pg:
        with SapSession() as sap:
            for ts, run_id, summary in source_rows(pg, inicio, fin, batch_size, pi_id, limit):
                counters["leidos"] += 1
                try:
                    # Usar exactamente el ts y run_id recuperados del padre PostgreSQL.
                    done = pg.execute("SELECT consultado FROM iislog.iis_mpl_id WHERE ts=%s AND run_id=%s", (ts, run_id)).fetchone()
                    if done and done[0] == "S":
                        counters["omitidos"] += 1
                        continue
                    mpl = extract_mpl(summary, os.environ.get("MPL_JSON_POINTER", ""))
                    pg.execute("""INSERT INTO iislog.iis_mpl_id
                        (ts, run_id, texto_mpl_id, texto_legible, consultado)
                        VALUES (%s, %s, %s, NULL, 'N')
                        ON CONFLICT (ts, run_id) DO UPDATE SET texto_mpl_id=EXCLUDED.texto_mpl_id
                        WHERE iis_mpl_id.consultado='N'""", (ts, run_id, mpl))
                    message = sap.message(mpl)
                    # Una única sentencia: guardar mensaje y marcar S de forma atómica.
                    updated = pg.execute("""UPDATE iislog.iis_mpl_id SET texto_legible=%s, texto_cpi=%s, consultado='S'
                        WHERE ts=%s AND run_id=%s AND texto_mpl_id=%s AND consultado='N'""",
                        (message.texto_legible, message.texto_cpi, ts, run_id, mpl))
                    if updated.rowcount != 1:
                        raise ProcessingError("El registro cambió durante la consulta; vuelve a ejecutar para verificar su estado")
                    counters["guardados"] += 1
                    LOG.info("pi_id=%s | run_id=%s | MPL-ID=%s | message=%s", pi_id, run_id, mpl, message.texto_legible)
                except AuthenticationError:
                    raise  # No continuar miles de registros con sesión vencida.
                except Exception as exc:
                    counters["fallidos"] += 1
                    # Mostrar errores propios, pero evitar imprimir DSN/SQL/cookies de excepciones externas.
                    reason = str(exc) if isinstance(exc, ProcessingError) else type(exc).__name__
                    LOG.error("pi_id=%s | run_id=%s: %s. No se marca como consultado.", pi_id, run_id, reason)
    LOG.info("pi_id=%s | Resultado: %s", pi_id, counters)
    return 1 if counters["fallidos"] else 0


def run(fecha, limit):
    day_zone = ZoneInfo(os.environ.get("DATE_TIMEZONE", "America/Lima"))
    inicio = datetime.combine(fecha, datetime.min.time(), tzinfo=day_zone)
    fin = datetime.combine(fecha + timedelta(days=1), datetime.min.time(), tzinfo=day_zone)
    batch_size = int(os.environ.get("PG_FETCH_SIZE", "200"))
    if batch_size < 1:
        raise ProcessingError("PG_FETCH_SIZE debe ser mayor que cero")
    LOG.info("Procesando fecha %s (%s), pi_id=%s, status=ERROR", fecha, day_zone, ", ".join(PI_IDS))
    result = 0
    with ThreadPoolExecutor(max_workers=len(PI_IDS), thread_name_prefix="mpl") as workers:
        futures = {workers.submit(run_pi, pi_id, inicio, fin, batch_size, limit): pi_id for pi_id in PI_IDS}
        for future in as_completed(futures):
            pi_id = futures[future]
            try:
                result |= future.result()
            except Exception as exc:
                reason = str(exc) if isinstance(exc, ProcessingError) else type(exc).__name__
                LOG.error("pi_id=%s: trabajador detenido: %s", pi_id, reason)
                result = 1
    return result


def self_test():
    import threading
    from unittest.mock import patch

    mpl = "AGrH3H1b_fk4Eo4YHU1MuZZ3R8Gn"
    example_base = "https://example.invalid"
    assert parse_fecha("09/10/2026") == date(2026, 10, 9)
    try:
        parse_fecha("9/10/2026")
    except argparse.ArgumentTypeError:
        pass
    else:
        raise AssertionError("Debe exigir DD/MM/YYYY con dos dígitos para día y mes")
    assert extract_mpl({"meta": {"MPL-ID": mpl}}) == mpl
    assert extract_mpl({"failed_items": [{"body": "The MPL ID for the failed message is : " + mpl}]}) == mpl
    assert extract_mpl({"failed_items": [{"body": json.dumps({"error": "MPL-ID: " + mpl})}]}) == mpl
    assert extract_mpl(json.dumps({"payload": {"id": mpl}}), "/payload/id") == mpl
    assert extract_mpl({"url": example_base + '/shell/monitoring/Messages/%7B%22identifier%22%3A%22' + mpl + '%22%7D'}) == mpl
    assert extract_message('{"error":{"message":{"lang":"en","value":"Address is incomplete."}}}') == "Address is incomplete."
    assert extract_message('{"error":{"message":"Error simple"}}') == "Error simple"
    xml = '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:m="urn:m" xmlns:d="urn:d"><entry><content><m:properties><d:Id>123</d:Id><d:Name>HTTP_Receiver_Adapter_Response_Body</d:Name></m:properties></content></entry><link rel="next" href="?page=2"/></feed>'
    items, nxt = parse_attachments(xml)
    assert items[0]["Id"] == "123" and nxt == "?page=2"
    assert parse_attachments('{"d":{"results":[{"Id":"123","Name":"test"}],"__next":"?p=2"}}')[1] == "?p=2"
    # Prueba integral del enlace: usa d:Id del adjunto correcto, nunca el id Atom.
    fake = SapSession()
    fake.base = example_base
    urls = []
    def fake_get(url):
        urls.append(url)
        if url.endswith("/Attachments"):
            return xml.replace('<link rel="next" href="?page=2"/>', '')
        assert url.endswith("MessageProcessingLogAttachments('123')/$value")
        return '{"error":{"message":{"value":"Mensaje obtenido"}}}'
    fake.get = fake_get
    assert fake.message(mpl) == SapMessage("Mensaje obtenido", "Mensaje obtenido") and len(urls) == 2
    def timed_out_attachment(url):
        if url.endswith("/Attachments"):
            return xml.replace('<link rel="next" href="?page=2"/>', '')
        assert url.endswith("MessageProcessingLogAttachments('123')/$value")
        return '{"error":{"message":{"value":"Read timed out"}}}'

    fake.get = timed_out_attachment
    assert fake.message(mpl) == SapMessage("Read timed out", "Read timed out")
    urls.clear()

    def fallback_get(url):
        urls.append(url)
        if url.endswith("/Attachments"):
            return '{"d":{"results":[{"Name":"Otro adjunto","Id":"999"}],"__next":"?page=2"}}'
        if url.endswith("?page=2"):
            return '{"d":{"results":[]}}'
        assert url.endswith("MessageProcessingLogs('" + mpl + "')/ErrorInformation/$value")
        return "\ufeff  Error de integración: datos incompletos.\nDetalle técnico.\nThe MPL ID for the failed message is : " + mpl

    fake.get = fallback_get
    full_message = "\ufeff  Error de integración: datos incompletos.\nDetalle técnico.\nThe MPL ID for the failed message is : " + mpl
    assert fake.message(mpl) == SapMessage(REVIEW_CPI_MESSAGE, full_message)
    assert len(urls) == 3 and urls[-1].endswith("/ErrorInformation/$value")
    exception_text = "com.sap.it.rt.adapter.http.api.exception.HttpResponseException: java.net.SocketTimeoutException: Read timed out"
    timeout_body = exception_text + "\nThe MPL ID for the failed message is : " + mpl
    assert extract_error_information(timeout_body) == timeout_body
    fake.get = lambda url: '{"d":{"results":[]}}' if url.endswith("/Attachments") else timeout_body
    assert fake.message(mpl) == SapMessage(REVIEW_CPI_MESSAGE, timeout_body)
    assert extract_error_information("The MPL ID for the failed message is : " + mpl) == "The MPL ID for the failed message is : " + mpl

    def attachment_without_id(url):
        assert not url.endswith("/ErrorInformation/$value")
        return '{"d":{"results":[{"Name":"' + ATTACHMENT_NAME + '"}]}}'

    fake.get = attachment_without_id
    try:
        fake.message(mpl)
    except ProcessingError:
        pass
    else:
        raise AssertionError("Un adjunto presente sin Id no debe activar ErrorInformation")
    try:
        extract_error_information(" \ufeff \n ")
    except ProcessingError:
        pass
    else:
        raise AssertionError("ErrorInformation vacío no debe marcarse como consultado")
    for invalid in ({}, {"mpl_id": mpl, "MPL-ID": "AGnigGTSbMdWZmN6mrT7rd9o6TD4"}):
        try:
            extract_mpl(invalid)
        except ProcessingError:
            pass
        else:
            raise AssertionError("Debe rechazar MPL-ID ausente o ambiguo")
    try:
        extract_message('<html>Login</html>')
    except ProcessingError:
        pass
    else:
        raise AssertionError("Debe rechazar HTML de login")
    class FakePg:
        def __init__(self):
            self.params = None

        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql, params):
            assert "r.summary_json ->> 'pi_id' = %s AND r.status = %s" in sql
            self.params = params
            return self

        def fetchall(self):
            return [("ts", "run_id", {})]

    for pi_id in PI_IDS:
        fake_pg = FakePg()
        assert len(list(source_rows(fake_pg, "inicio", "fin", 1, pi_id, 1))) == 1
        assert fake_pg.params == ["inicio", "fin", pi_id, "ERROR", 1]

    barrier = threading.Barrier(len(PI_IDS))
    called = []

    def fake_worker(pi_id, inicio, fin, batch_size, limit):
        called.append((pi_id, threading.get_ident(), limit))
        barrier.wait(timeout=5)
        return 0

    with patch.object(sys.modules[__name__], "run_pi", fake_worker), patch.object(sys.modules[__name__], "ZoneInfo", lambda _: timezone.utc):
        assert run(date(2026, 10, 9), 1) == 0
    assert {x[0] for x in called} == set(PI_IDS)
    assert len({x[1] for x in called}) == len(PI_IDS)
    assert all(x[2] == 1 for x in called)
    print("Pruebas locales correctas: MPL-ID, adjunto, ErrorInformation, filtros y tres hilos independientes.")
    return 0


def parse_fecha(value):
    if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
        raise argparse.ArgumentTypeError("La fecha debe tener formato DD/MM/YYYY")
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("La fecha debe tener formato DD/MM/YYYY") from exc


def configure_logging():
    zone = ZoneInfo(os.environ.get("DATE_TIMEZONE", "America/Lima"))
    log_dir = Path(os.environ.get("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(zone).strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"consultar_mpl_{stamp}_{os.getpid()}.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")])
    LOG.info("Log de ejecución: %s", log_path.resolve())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default=".env.postgresql", help="Ruta del archivo de configuración")
    parser.add_argument("--init-env", action="store_true", help="Crear plantilla de configuración sin sobrescribir")
    parser.add_argument("--fecha", type=parse_fecha, help="Fecha DD/MM/YYYY en DATE_TIMEZONE; si se omite, se solicita")
    parser.add_argument("--mpl-id", help="Solo prueba SAP; no escribe en bases de datos")
    parser.add_argument("--limit", type=int, help="Máximo de registros leídos por pi_id")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.init_env:
        with Path(args.env).open("x", encoding="utf-8") as output:
            output.write(ENV_TEMPLATE)
        print(f"Plantilla creada: {args.env}")
        return 0
    if args.self_test:
        return self_test()
    if args.fecha and args.mpl_id:
        parser.error("Usa --fecha o --mpl-id (solo uno)")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit debe ser mayor que cero")
    from dotenv import load_dotenv
    load_dotenv(args.env, override=False, interpolate=False)
    configure_logging()
    if args.mpl_id:
        with SapSession() as sap:
            LOG.info("MPL-ID=%s | message=%s", args.mpl_id, sap.message(args.mpl_id).texto_legible)
        return 0
    fecha = args.fecha
    if fecha is None:
        try:
            fecha = parse_fecha(input("Fecha (DD/MM/YYYY): ").strip())
        except (EOFError, argparse.ArgumentTypeError) as exc:
            parser.error(str(exc) if str(exc) else "Debes ingresar la fecha en DD/MM/YYYY")
    return run(fecha, args.limit)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ProcessingError) else type(exc).__name__
        if logging.getLogger().handlers:
            LOG.error("%s", reason)
        else:
            print(f"ERROR: {reason}", file=sys.stderr)
        raise SystemExit(1)

