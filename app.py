from __future__ import annotations

import os

from flask import Flask, flash, render_template, request

from document_comparator import compare_uploaded_files


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_UPLOAD_MB", "250")) * 1024 * 1024
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")


@app.route("/", methods=["GET", "POST"])
def index() -> str:
    comparison = None

    if request.method == "POST":
        file_a = request.files.get("file_a")
        file_b = request.files.get("file_b")

        if not file_a or not file_b or not file_a.filename or not file_b.filename:
            flash("Envie dois arquivos para comparar.", "error")
        else:
            comparison = compare_uploaded_files(file_a, file_b)

    return render_template("index.html", comparison=comparison)


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG") == "1"
    app.run(debug=debug, use_reloader=False)
