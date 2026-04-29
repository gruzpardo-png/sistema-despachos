function pedirMotivo(form){
  const motivo = prompt("Indica el motivo de anulación:");
  if(!motivo || !motivo.trim()){
    alert("La anulación requiere motivo.");
    return false;
  }
  form.querySelector('input[name="motivo_anulacion"]').value = motivo.trim();
  return confirm("¿Confirmas anular este registro?");
}