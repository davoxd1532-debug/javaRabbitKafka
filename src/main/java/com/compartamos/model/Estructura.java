package com.compartamos.model;

import java.io.Serializable;

public class Estructura implements Serializable {
    private String canal;
    private int empresa;
    private int sucursal;
    private int modulo;
    private int transaccion;
    private int relacion;
    private String fecha;
    private int tipoDocumento;
    private int pais;
    private String numeroDocumento;
    private String tipo;
    private String subTipo;
    private String hora;
    private String horafirma;
    private String workstation;
    private String usuario;
    private int usuarioSucursal;
    private String trace;
    private String aux1;
    private String aux2;
    private String aux3;
    private double aux4;

    public String getCanal() {
        return canal;
    }

    public void setCanal(String canal) {
        this.canal = canal;
    }

    public int getEmpresa() {
        return empresa;
    }

    public void setEmpresa(int empresa) {
        this.empresa = empresa;
    }

    public int getSucursal() {
        return sucursal;
    }

    public void setSucursal(int sucursal) {
        this.sucursal = sucursal;
    }

    public int getModulo() {
        return modulo;
    }

    public void setModulo(int modulo) {
        this.modulo = modulo;
    }

    public int getTransaccion() {
        return transaccion;
    }

    public void setTransaccion(int transaccion) {
        this.transaccion = transaccion;
    }

    public int getRelacion() {
        return relacion;
    }

    public void setRelacion(int relacion) {
        this.relacion = relacion;
    }

    public String getFecha() {
        return fecha;
    }

    public void setFecha(String fecha) {
        this.fecha = fecha;
    }

    public int getPais() {
        return pais;
    }

    public void setPais(int pais) {
        this.pais = pais;
    }

    public int getTipoDocumento() {
        return tipoDocumento;
    }

    public void setTipoDocumento(int tipoDocumento) {
        this.tipoDocumento = tipoDocumento;
    }

    public String getNumeroDocumento() {
        return numeroDocumento;
    }

    public void setNumeroDocumento(String numeroDocumento) {
        this.numeroDocumento = numeroDocumento;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getSubTipo() {
        return subTipo;
    }

    public void setSubTipo(String subTipo) {
        this.subTipo = subTipo;
    }

    public String getHora() {
        return hora;
    }

    public void setHora(String hora) {
        this.hora = hora;
    }

    public String getHoraFirma() {
        return horafirma;
    }

    public void setHoraFirma(String horafirma) {
        this.horafirma = horafirma;
    }

    public String getWorkstation() {
        return workstation;
    }

    public void setWorkstation(String workstation) {
        this.workstation = workstation;
    }

    public String getUsuario() {
        return usuario;
    }

    public void setUsuario(String usuario) {
        this.usuario = usuario;
    }

    public int getUsuarioSucursal() {
        return usuarioSucursal;
    }

    public void setUsuarioSucursal(int usuarioSucursal) {
        this.usuarioSucursal = usuarioSucursal;
    }

    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }

    public String getAux1() {
        return aux1;
    }

    public void setAux1(String aux1) {
        this.aux1 = aux1;
    }

    public String getAux2() {
        return aux2;
    }

    public void setAux2(String aux2) {
        this.aux2 = aux2;
    }

    public String getAux3() {
        return aux3;
    }

    public void setAux3(String aux3) {
        this.aux3 = aux3;
    }

    public double getAux4() {
        return aux4;
    }

    public void setAux4(double aux4) {
        this.aux4 = aux4;
    }
}
