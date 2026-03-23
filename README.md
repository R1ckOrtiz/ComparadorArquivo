# Inspetor de Documentos

Aplicação em Flask para comparar duas versões de arquivos.

## O que o MVP faz

- aceita upload de qualquer extensão;
- compara com diff detalhado quando consegue extrair conteúdo legível;
- suporta `txt`, `json`, `csv`, `xml`, `svg`, `pdf`, `docx` e `xlsx`;
- usa comparação estrutural para arquivos ZIP e formatos baseados em ZIP não suportados diretamente;
- usa fallback binário para o restante.

## Como executar

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Abra `http://127.0.0.1:5000`.

## Observações

- O suporte a "qualquer extensão" significa aceitar o upload de qualquer arquivo.
- A explicação detalhada das mudanças depende de um parser para o formato.
- Arquivos escaneados sem texto embutido podem precisar de OCR em uma próxima fase.
