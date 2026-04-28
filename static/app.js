document.addEventListener('DOMContentLoaded', () => {
  const flashes = document.querySelectorAll('.flash');
  flashes.forEach((flash) => {
    setTimeout(() => {
      flash.style.transition = 'opacity .35s ease, transform .35s ease';
      flash.style.opacity = '0';
      flash.style.transform = 'translateY(-6px)';
    }, 6500);
  });

  const cancelForms = document.querySelectorAll('.cancel-form');
  cancelForms.forEach((form) => {
    form.addEventListener('submit', (event) => {
      const reason = form.querySelector('input[name="cancelled_reason"]')?.value?.trim();
      if (!reason) {
        event.preventDefault();
        alert('Debes ingresar el motivo de anulación.');
        return;
      }
      if (!confirm('¿Confirmas anular este registro? Esta acción quedará en auditoría.')) {
        event.preventDefault();
      }
    });
  });
});
