ERROR ACTUAL:
Render muestra: File "/opt/render/project/src/app.py", line 1 {% extends "base.html" %}
Eso significa que app.py quedó con contenido HTML/Jinja. app.py debe contener código Python.

ESTRUCTURA CORRECTA:
app.py                                <-- Python Flask, empieza con: import os
requirements.txt                       <-- solo librerías Python
render.yaml
static/logo.png
static/styles.css
static/app.js
templates/base.html                    <-- aquí sí pueden ir líneas {% extends %} o bloques Jinja
templates/login.html
templates/despachos.html
templates/consultas.html
templates/dashboard.html
templates/users.html

NO PEGAR ningún archivo .html dentro de app.py.
NO PEGAR README dentro de requirements.txt.
