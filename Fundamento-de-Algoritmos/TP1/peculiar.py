#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 13:17:37 2023

@author: alessiobocco
"""

def misma_paridad(n, m):
    """
    Esta función verifica si dos números n y m tienen la misma paridad, es decir,
    si ambos son pares o ambos son impares.

    Argumentos:
    n (int): Primer número entero.
    m (int): Segundo número entero.

    Retorno:
    bool: True si n y m tienen la misma paridad, False en caso contrario.
    """
    # Verifica si los argumentos son enteros y lanza un error si no lo son
    if not isinstance(n, int) or not isinstance(m, int):
        raise ValueError("Ambos argumentos deben ser números enteros")

    # Comprueba si n y m tienen la misma paridad usando el operador módulo (%)
    # y comparando si ambos resultados son iguales (ambos pares o ambos impares)
    return (n % 2 == 0) == (m % 2 == 0)

def alterna_paridad(n):
    """
    Esta función verifica si los dígitos de un número entero n alternan su paridad,
    es decir, si un dígito par es seguido por un dígito impar y viceversa.

    Argumentos:
    n (int): Número entero 

    Retorno / Return:
    bool: True si los dígitos de n alternan su paridad, False en caso contrario 
    """

    # Verifica si el argumento es un entero y lanza un error si no lo es
    if not isinstance(n, int):
        raise ValueError("El argumento debe ser un número entero")

    # Convierte n en una cadena para iterar sobre sus dígitos
    str_n = str(n)

    # Itera sobre los dígitos de n, verificando si alternan su paridad
    for i in range(len(str_n) - 1):
        if (int(str_n[i]) % 2 == 0) == (int(str_n[i + 1]) % 2 == 0):
            return False

    return True

def es_peculiar(n):
    """
    Esta función verifica si un número entero n es peculiar, es decir,
    si n es múltiplo de 22 y sus dígitos alternan su paridad.

    Argumentos:
    n (int): Número entero 

    Retorno / Return:
    bool: True si n es peculiar, False en caso contrario 
    """

    # Verifica si el argumento es un entero y lanza un error si no lo es
    if not isinstance(n, int):
        raise ValueError("El argumento debe ser un número entero")

    # Verifica si n es múltiplo de 22 y si sus dígitos alternan su paridad
    return (n % 22 == 0) and alterna_paridad(n)


def n_esimo_peculiar(n):
    """
    Esta función encuentra el n-ésimo número peculiar.

    Argumentos:
    n (int): Índice del número peculiar deseado 

    Retorno / Return:
    int: El n-ésimo número peculiar
    """

    # Verifica si el argumento es un entero y lanza un error si no lo es
    if not isinstance(n, int):
        raise ValueError("El argumento debe ser un número entero")

    count = -1
    num = -1
    while count < n:
        num += 1
        if es_peculiar(num):
            count += 1
    return num


def cant_peculiares_entre(n, m):
    """
    Esta función cuenta la cantidad de números peculiares entre dos números enteros n y m, inclusive.

    Argumentos:
    n (int): Límite inferior del rango 
    m (int): Límite superior del rango

    Retorno / Return:
    int: Cantidad de números peculiares en el rango [n, m] 
    """

    # Verifica si los argumentos son enteros y lanza un error si no lo son
    if not isinstance(n, int) or not isinstance(m, int):
        raise ValueError("Ambos argumentos deben ser números enteros")

    count = 0
    for num in range(n, m + 1):
        if es_peculiar(num):
            count += 1
    return count

def solicitar_entero(prompt):
    """
    Esta función solicita al usuario un número entero y valida su entrada.
    Si la entrada es válida, devuelve el número entero y None.
    Si la entrada no es válida, devuelve None y un mensaje de error.

    Argumentos:
    prompt (str): Mensaje que se muestra al solicitar la entrada del usuario.

    Retorno:
    (int, None) si la entrada es válida, (None, str) si la entrada no es válida.
    """

    # Bucle infinito que se ejecuta hasta que se ingrese una entrada válida
    while True:
        # Solicita la entrada del usuario utilizando el mensaje 'prompt'
        entrada_usuario = input(prompt)

        try:
            # Intenta convertir la entrada del usuario a un número entero
            numero_entero = int(entrada_usuario)
            
            # Si la conversión es exitosa, devuelve el número entero y None
            return numero_entero, None
        except ValueError:
            # Si la conversión falla, devuelve None y un mensaje de error
            error_message = "La entrada no es un número entero válido. Por favor, intente de nuevo."
            return None, error_message


