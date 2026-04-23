from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size)


def main() -> None:
    output_path = Path("data/source/pdf_with_text_table_image.pdf")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    font_path = "/System/Library/Fonts/Supplemental/Arial.ttf"
    bold_path = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"

    title_font = font(bold_path, 54)
    body_font = font(font_path, 34)
    small_font = font(font_path, 28)

    image = Image.new("RGB", (1240, 1754), "white")
    draw = ImageDraw.Draw(image)

    draw.text((80, 80), "PDF de Teste - Texto, Tabela e Imagem", font=title_font, fill="black")
    draw.text(
        (80, 170),
        "Este PDF simula um documento com texto corrido, uma tabela simples e uma imagem embutida.",
        font=small_font,
        fill="black",
    )
    draw.text((80, 220), "Cliente: MPI Document", font=body_font, fill="black")
    draw.text((80, 270), "Data: 2026-04-23", font=body_font, fill="black")

    draw_table(draw, small_font)
    draw_embedded_image(draw, body_font, small_font)

    draw.text((80, 1220), "Total geral: 270.00", font=body_font, fill="black")
    image.save(output_path, "PDF", resolution=150.0)
    print(output_path)


def draw_table(draw: ImageDraw.ImageDraw, small_font: ImageFont.FreeTypeFont) -> None:
    x0, y0 = 80, 380
    row_h = 70
    width = 1000
    columns = [0, 280, 560, 820, width]
    headers = ["Item", "Descricao", "Qtd", "Valor"]
    rows = [
        ["1", "Servico OCR", "2", "150.00"],
        ["2", "Validacao", "1", "80.00"],
        ["3", "Exportacao", "1", "40.00"],
    ]

    total_rows = len(rows) + 1
    draw.rectangle((x0, y0, x0 + width, y0 + row_h * total_rows), outline="black", width=3)

    for column_x in columns[1:-1]:
        draw.line((x0 + column_x, y0, x0 + column_x, y0 + row_h * total_rows), fill="black", width=2)

    for row_index in range(1, total_rows):
        y = y0 + row_h * row_index
        draw.line((x0, y, x0 + width, y), fill="black", width=2)

    for index, value in enumerate(headers):
        draw.text((x0 + columns[index] + 18, y0 + 18), value, font=small_font, fill="black")

    for row_index, row in enumerate(rows, start=1):
        for column_index, value in enumerate(row):
            draw.text(
                (x0 + columns[column_index] + 18, y0 + row_h * row_index + 18),
                value,
                font=small_font,
                fill="black",
            )


def draw_embedded_image(
    draw: ImageDraw.ImageDraw,
    body_font: ImageFont.FreeTypeFont,
    small_font: ImageFont.FreeTypeFont,
) -> None:
    draw.text((80, 760), "Imagem embutida:", font=body_font, fill="black")
    draw.rectangle((80, 830, 520, 1120), fill=(230, 242, 255), outline="black", width=2)
    draw.ellipse((130, 890, 250, 1010), fill=(80, 130, 220))
    draw.rectangle((300, 910, 470, 1030), fill=(40, 160, 110))
    draw.text((120, 1060), "Figura demonstrativa", font=small_font, fill="black")


if __name__ == "__main__":
    main()
