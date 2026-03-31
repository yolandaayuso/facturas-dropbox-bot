import os
import re
import io
import csv
import json
import tempfile
import subprocess
from pathlib import Path

import requests
from PIL import Image
from pypdf import PdfReader

APP_KEY = os.environ["DROPBOX_APP_KEY"]
APP_SECRET = os.environ["DROPBOX_APP_SECRET"]
REFRESH_TOKEN = os.environ["DROPBOX_REFRESH_TOKEN"]

INPUT_FOLDER = "/Entrada"
PROCESSED_FOLDER = "/Procesadas"
ERROR_FOLDER = "/Error"
OUTPUT_CSV = "/Salidas/gastos.csv"


def get_access_token():
    r = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
            "client_id": APP_KEY,
            "client_secret": APP_SECRET,
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if "access_token" not in data:
        raise RuntimeError(f"No se pudo obtener access_token: {data}")
    return data["access_token"]


def dbx_api(token, endpoint, payload):
    r = requests.post(
        f"https://api.dropboxapi.com/2/{endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=120,
    )
    r.raise_for_status()
    return r.json() if r.text else {}


def dbx_list_folder(token, path):
    data = dbx_api(token, "files/list_folder", {"path": path})
    return data.get("entries", [])


def dbx_download(token, path, target_path):
    r = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers={
            "Authorization": f"Bearer {token}",
            "Dropbox-API-Arg": json.dumps({"path": path}),
        },
        timeout=300,
    )
    r.raise_for_status()
    target_path.write_bytes(r.content)


def dbx_download_if_exists(token, path):
    r = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers={
            "Authorization": f"Bearer {token}",
            "Dropbox-API-Arg": json.dumps({"path": path}),
        },
        timeout=120,
    )
    if r.status_code == 409:
        return b""
    r.raise_for_status()
    return r.content


def dbx_upload(token, content, path):
    r = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers={
            "Authorization": f"Bearer {token}",
            "Dropbox-API-Arg": json.dumps({
                "path": path,
                "mode": "overwrite",
                "autorename": False,
                "mute": True,
            }),
            "Content-Type": "application/octet-stream",
        },
        data=content,
        timeout=300,
    )
    r.raise_for_status()
    return r.json()


def dbx_move(token, from_path, to_path):
    return dbx_api(token, "files/move_v2", {
        "from_path": from_path,
        "to_path": to_path,
        "autorename": True,
        "allow_shared_folder": True,
        "allow_ownership_transfer": False,
    })


def image_to_pdf(image_path, output_pdf):
    img = Image.open(image_path)
    if img.mode != "RGB":
        img = img.convert("RGB")
    img.save(output_pdf, "PDF", resolution=300.0)


def run_ocr(input_pdf, output_pdf):
    subprocess.run(
        ["ocrmypdf", "--skip-text", "--force-ocr", str(input_pdf), str(output_pdf)],
        check=True
    )


def pdf_to_text(pdf_path):
    reader = PdfReader(str(pdf_path))
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)


def try_invoice2data(pdf_path):
    p = subprocess.run(
        ["invoice2data", "--output-format", "json", str(pdf_path)],
        capture_output=True,
        text=True
    )
    if p.returncode != 0 or not p.stdout.strip():
        return {}
    try:
        data = json.loads(p.stdout)
        if isinstance(data, list):
            return data[0] if data else {}
        return data
    except Exception:
        return {}


def normalize_amount(value):
    v = value.strip().replace("€", "").replace(" ", "")
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    elif "," in v:
        v = v.replace(".", "").replace(",", ".")
    return v


def extract_with_regex(text):
    clean = re.sub(r"[ \t]+", " ", text)
    out = {}

    for pat in [r"\b(\d{2}[/-]\d{2}[/-]\d{4})\b", r"\b(\d{4}[/-]\d{2}[/-]\d{2})\b"]:
        m = re.search(pat, clean)
        if m:
            out["date"] = m.group(1)
            break

    for pat in [
        r"(?:factura|invoice|n[ºo°])[\\s:.-]*([A-Z0-9\\-/]+)",
        r"(?:n[uú]mero de factura)[\\s:.-]*([A-Z0-9\\-/]+)",
    ]:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            out["invoice_number"] = m.group(1)
            break

    patterns = {
        "base": [
            r"(?:base imponible)[^\d]{0,10}([\d\.\,]+)",
            r"(?:subtotal)[^\d]{0,10}([\d\.\,]+)",
        ],
        "iva": [
            r"(?:iva)[^\d]{0,10}([\d\.\,]+)",
        ],
        "total": [
            r"(?:importe total|total factura|total)[^\d]{0,10}([\d\.\,]+)",
        ],
    }

    for field, pats in patterns.items():
        for pat in pats:
            m = re.search(pat, clean, re.IGNORECASE)
            if m:
                out[field] = normalize_amount(m.group(1))
                break

    for line in text.splitlines():
        line = line.strip()
        if len(line) > 3 and len(line) < 80:
            if not re.search(r"\d{2}[/-]\d{2}[/-]\d{4}", line):
                out.setdefault("supplier", line)
                break

    return out


def merge_data(primary, fallback, dropbox_path):
    def pick(*keys):
        for d in (primary, fallback):
            for k in keys:
                if d.get(k):
                    return d[k]
        return ""

    return {
        "fecha": pick("date", "invoice_date"),
        "proveedor": pick("supplier", "issuer", "seller"),
        "numero_factura": pick("invoice_number", "invoice_no", "number"),
        "base_imponible": pick("base", "subtotal", "amount_untaxed"),
        "iva": pick("iva", "tax", "amount_tax"),
        "total": pick("total", "amount", "amount_total"),
        "archivo_dropbox": dropbox_path,
    }


def read_existing_rows(csv_bytes):
    if not csv_bytes:
        return []
    text = csv_bytes.decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def write_csv(rows):
    output = io.StringIO()
    fieldnames = ["fecha", "proveedor", "numero_factura", "base_imponible", "iva", "total", "archivo_dropbox"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=";")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8-sig")


def process_file(token, entry):
    path_display = entry["path_display"]
    file_name = entry["name"]
    suffix = Path(file_name).suffix.lower()

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        original = td / file_name
        dbx_download(token, path_display, original)

        pdf_input = original
        if suffix in [".jpg", ".jpeg", ".png"]:
            pdf_input = td / f"{original.stem}.pdf"
            image_to_pdf(original, pdf_input)
        elif suffix != ".pdf":
            raise ValueError(f"Formato no soportado: {suffix}")

        ocr_pdf = td / f"{pdf_input.stem}_ocr.pdf"
        run_ocr(pdf_input, ocr_pdf)

        inv = try_invoice2data(ocr_pdf)
        text = pdf_to_text(ocr_pdf)
        reg = extract_with_regex(text)

        return merge_data(inv, reg, path_display)


def main():
    token = get_access_token()
    entries = dbx_list_folder(token, INPUT_FOLDER)
    files = [e for e in entries if e.get(".tag") == "file"]

    if not files:
        print("No hay archivos en Entrada")
        return

    rows = read_existing_rows(dbx_download_if_exists(token, OUTPUT_CSV))

    for entry in files:
        src = entry["path_display"]
        dest_name = entry["name"]

        try:
            row = process_file(token, entry)
            rows.append(row)
            dbx_move(token, src, f"{PROCESSED_FOLDER}/{dest_name}")
            print(f"OK: {dest_name}")
        except Exception as e:
            dbx_move(token, src, f"{ERROR_FOLDER}/{dest_name}")
            print(f"ERROR: {dest_name} -> {e}")

    dbx_upload(token, write_csv(rows), OUTPUT_CSV)
    print("CSV actualizado correctamente")


if __name__ == "__main__":
    main()
