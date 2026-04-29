{% extends "base.html" %}
{% block title %}Usuarios | Ferretería San Pedro{% endblock %}
{% block content %}
<section class="hero"><div><h1>Usuarios</h1><p>Administra accesos, roles y permisos del sistema.</p></div></section>
<section class="grid-two user-layout">
  <article class="card">
    <h2>Crear usuario</h2>
    <form method="post" class="form-stack">
      <label>Nombre<input name="name" required placeholder="Nombre trabajador"></label>
      <label>Usuario<input name="username" required placeholder="ej: camilo_llanca"></label>
      <label>Clave<input name="password" type="password" required minlength="6"></label>
      <label>Rol<select name="role">{% for role in roles %}<option value="{{ role }}">{{ role }}</option>{% endfor %}</select></label>
      <button class="primary" type="submit">Crear usuario</button>
    </form>
  </article>
  <article class="card">
    <h2>Usuarios existentes</h2>
    <div class="table-wrap compact-table"><table><thead><tr><th>Nombre</th><th>Usuario</th><th>Rol</th><th>Estado</th><th>Acción</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.name }}</td><td>{{ u.username }}</td><td>{{ u.role }}</td><td>{% if u.active %}<span class="status entregado">ACTIVO</span>{% else %}<span class="status anulado">INACTIVO</span>{% endif %}</td><td>{% if u.id != current_user.id %}<form method="post" action="{{ url_for('toggle_user', user_id=u.id) }}"><button class="secondary" type="submit">{% if u.active %}Desactivar{% else %}Activar{% endif %}</button></form>{% else %}<span class="muted">Tu usuario</span>{% endif %}</td></tr>{% endfor %}</tbody></table></div>
  </article>
</section>
{% endblock %}
