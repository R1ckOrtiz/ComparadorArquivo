from __future__ import annotations

import io
import unittest
import zipfile

from openpyxl import Workbook

from document_comparator import compare_documents


class DocumentComparatorTests(unittest.TestCase):
    def test_text_diff_detects_changes(self) -> None:
        result = compare_documents(
            "versao_a.txt",
            b"linha 1\nlinha 2\nlinha 3",
            "versao_b.txt",
            b"linha 1\nlinha alterada\nlinha 3",
        )

        self.assertEqual(result["mode"], "text")
        self.assertFalse(result["identical"])
        self.assertEqual(result["summary"]["changed"], 1)
        self.assertIn("linha alterada", result["unified_diff"])

    def test_json_normalization_ignores_key_order(self) -> None:
        result = compare_documents(
            "a.json",
            b'{"b": 2, "a": 1}',
            "b.json",
            b'{\n  "a": 1,\n  "b": 2\n}',
        )

        self.assertTrue(result["identical"])
        self.assertEqual(result["summary"]["differences"], 0)

    def test_json_diff_reports_exact_field_path(self) -> None:
        result = compare_documents(
            "a.json",
            b'{"cliente": {"nome": "Ana", "ativo": true}}',
            "b.json",
            b'{"cliente": {"nome": "Bia", "ativo": true}}',
        )

        self.assertEqual(result["mode"], "structured")
        self.assertEqual(result["changes"][0]["path"], "$.cliente.nome")
        self.assertEqual(result["changes"][0]["location_label"], "Campo JSON cliente > nome")
        self.assertIn("Ana", result["changes"][0]["inline_diff"])
        self.assertIn("Bia", result["changes"][0]["inline_diff"])
        self.assertIn("diferença", result["overview"]["title"].lower())

    def test_csv_diff_reports_exact_cell(self) -> None:
        result = compare_documents(
            "a.csv",
            b"id,nome\n1,Ana\n2,Caio",
            "b.csv",
            b"id,nome\n1,Ana\n2,Carla",
        )

        self.assertEqual(result["mode"], "structured")
        self.assertEqual(result["changes"][0]["path"], "R3C2")
        self.assertEqual(result["changes"][0]["before_value"], "Caio")
        self.assertEqual(result["changes"][0]["after_value"], "Carla")

    def test_xml_diff_reports_exact_path(self) -> None:
        result = compare_documents(
            "a.xml",
            b"<pedido><item id='1'>Caneta</item></pedido>",
            "b.xml",
            b"<pedido><item id='2'>Caneta azul</item></pedido>",
        )

        self.assertEqual(result["mode"], "structured")
        changed_paths = {change["path"] for change in result["changes"]}
        self.assertIn("/pedido[1]/item[1]/@id", changed_paths)
        self.assertIn("/pedido[1]/item[1]/text()", changed_paths)
        self.assertTrue(any("Elemento XML" in change["location_label"] for change in result["changes"]))

    def test_zip_structure_fallback_reports_entry_changes(self) -> None:
        archive_a = build_zip({"manifest.txt": b"v1", "content/data.txt": b"abc"})
        archive_b = build_zip({"manifest.txt": b"v2", "content/data.txt": b"abc", "extra.txt": b"new"})

        result = compare_documents("bundle.one", archive_a, "bundle.two", archive_b)

        self.assertEqual(result["mode"], "archive")
        self.assertFalse(result["identical"])
        self.assertEqual(result["summary"]["added"], 1)
        self.assertEqual(result["summary"]["changed"], 1)

    def test_binary_fallback_reports_first_difference(self) -> None:
        result = compare_documents("a.bin", b"\x01\x02\x03\x04", "b.bin", b"\x01\x02\x09\x04")

        self.assertEqual(result["mode"], "binary")
        self.assertEqual(result["summary"]["first_difference_offset"], 2)
        self.assertEqual(result["summary"]["different_bytes"], 1)

    def test_text_diff_exposes_inline_diff(self) -> None:
        result = compare_documents(
            "a.txt",
            b"valor original",
            "b.txt",
            b"valor atualizado",
        )

        self.assertEqual(result["mode"], "text")
        self.assertIn("inline_diff", result["changes"][0])
        self.assertIn("[-original-]", result["changes"][0]["inline_diff"])
        self.assertIn("{+atualizado+}", result["changes"][0]["inline_diff"])

    def test_docx_extraction_compares_text(self) -> None:
        doc_a = build_minimal_docx("Primeira versao")
        doc_b = build_minimal_docx("Segunda versao")

        result = compare_documents("a.docx", doc_a, "b.docx", doc_b)

        self.assertEqual(result["mode"], "text")
        self.assertFalse(result["identical"])
        self.assertEqual(result["summary"]["changed"], 1)

    def test_xlsx_diff_reports_exact_cell(self) -> None:
        xlsx_a = build_xlsx({"Resumo": {"A1": "ID", "B1": "Nome", "A2": "1", "B2": "Ana"}})
        xlsx_b = build_xlsx({"Resumo": {"A1": "ID", "B1": "Nome", "A2": "1", "B2": "Bia"}})

        result = compare_documents("a.xlsx", xlsx_a, "b.xlsx", xlsx_b)

        self.assertEqual(result["mode"], "excel")
        self.assertEqual(result["changes"][0]["path"], "Resumo!B2")
        self.assertEqual(
            result["changes"][0]["location_label"],
            "Planilha \"Resumo\", linha 2, coluna B",
        )
        self.assertEqual(result["changes"][0]["column_name"], "Nome")
        self.assertIn("registro", result["changes"][0]["focus_label"])
        self.assertEqual(result["changes"][0]["before_value"], "Ana")
        self.assertEqual(result["changes"][0]["after_value"], "Bia")
        self.assertIn("mudou de", result["changes"][0]["details"])
        self.assertTrue(any(item["label"] == "Células alteradas" for item in result["summary_items"]))
        self.assertTrue(any(item["label"] == "Planilhas afetadas" for item in result["summary_items"]))

    def test_large_xlsx_diff_reports_exact_cell(self) -> None:
        base_cells = {"A1": "ID", "B1": "Nome"}
        for row in range(2, 1502):
            base_cells[f"A{row}"] = str(row - 1)
            base_cells[f"B{row}"] = f"cliente {row - 1}"

        changed_cells = dict(base_cells)
        changed_cells["B1400"] = "cliente atualizado"

        xlsx_a = build_xlsx({"Clientes": base_cells})
        xlsx_b = build_xlsx({"Clientes": changed_cells})

        result = compare_documents("grande_a.xlsx", xlsx_a, "grande_b.xlsx", xlsx_b)

        self.assertEqual(result["mode"], "excel")
        self.assertEqual(result["summary"]["changed"], 1)
        self.assertEqual(result["summary"]["rows_changed"], 1)
        self.assertEqual(result["summary"]["sheets_changed"], 1)
        self.assertEqual(result["changes"][0]["path"], "Clientes!B1400")
        self.assertEqual(result["changes"][0]["column_name"], "Nome")
        self.assertIn("row_context_before", result["changes"][0])
        self.assertIn("row_context_after", result["changes"][0])


def build_zip(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def build_minimal_docx(text: str) -> bytes:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""
    relationships = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""
    document = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r>
        <w:t>{text}</w:t>
      </w:r>
    </w:p>
  </w:body>
</w:document>
"""

    return build_zip(
        {
            "[Content_Types].xml": content_types.encode("utf-8"),
            "_rels/.rels": relationships.encode("utf-8"),
            "word/document.xml": document.encode("utf-8"),
        }
    )


def build_xlsx(sheets: dict[str, dict[str, str]]) -> bytes:
    workbook = Workbook()
    first_sheet = True

    for sheet_name, cells in sheets.items():
        if first_sheet:
            worksheet = workbook.active
            worksheet.title = sheet_name
            first_sheet = False
        else:
            worksheet = workbook.create_sheet(title=sheet_name)

        for cell_ref, value in cells.items():
            worksheet[cell_ref] = value

    buffer = io.BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


if __name__ == "__main__":
    unittest.main()
