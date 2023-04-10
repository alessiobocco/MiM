from peculiar import es_peculiar, n_esimo_peculiar, cant_peculiares_entre,\
                     misma_paridad, alterna_paridad, solicitar_entero

def elegir_funcion():
    '''
    Despliega el menú de funciones disponibles en la pantalla y devuelve
    la opción elegida por el usuario. La opción elegida por el usuario, en 
    mayúscula y sin espacios adelante y atrás según la siguiente codificación:
        A -> Misma paridad
        B -> Dígitos alternan paridad
        C -> Es peculiar
        D -> N-ésimo peculiar
        E -> Cantidad de peculiares entre
        F -> Finalizar
    '''
    print()
    print('Funciones disponibles')
    print('---------------------')
    print('A. Misma paridad [n,m]')
    print('B. Dígitos alternan paridad [n]')
    print('C. Es peculiar [n]')
    print('D. N-ésimo peculiar [n]')
    print('E. Cantidad de peculiares entre [n,m]')
    print('F. Finalizar')
    opción_elegida = input('Seleccione una opción: ').upper().strip()
    return opción_elegida


# Cuerpo principal del programa
finalizar = False
while not finalizar:
    opcion_seleccionada = elegir_funcion()
    # Se analiza la opción elegida
    if opcion_seleccionada == 'A':
        # Misma paridad
        while True:
            numero_n, mensaje_error_n = solicitar_entero("Ingrese el número entero n: ")
            if mensaje_error_n is not None:
                print(mensaje_error_n)
            else:
                break
        while True:
            numero_m, mensaje_error_m = solicitar_entero("Ingrese el número entero m: ")
            if mensaje_error_m is not None:
                print(mensaje_error_m)
            else:
                break
        resultado = misma_paridad(numero_n, numero_m)
        print(f"El {numero_n} y el {numero_m} {'tienen la misma paridad.' if resultado else 'no tienen la misma parida.'}") 
    elif opcion_seleccionada == 'B':
        # Alterna paridad
        while True:
            numero, mensaje_error = solicitar_entero("Ingrese el número entero n: ")
            if mensaje_error is not None:
                print(mensaje_error)
            else:
                break
        resultado = alterna_paridad(numero)
        print(f"Los digitos de {numero} {'alternan paridad.' if resultado else 'no alternan paridad.'}") 
    elif opcion_seleccionada == 'C':
        # Es peculiar
        while True:
            numero, mensaje_error = solicitar_entero("Ingrese el número entero n: ")
            if mensaje_error is not None:
                print(mensaje_error)
            else:
                break
        resultado = es_peculiar(numero)
        print(f"El número {numero} {'es peculiar.' if resultado else 'no es peculiar.'}") 
    elif opcion_seleccionada == 'D':
        # n_esimo_peculiar
        while True:
            numero, mensaje_error = solicitar_entero("Ingrese el número entero n: ")
            if mensaje_error is not None:
                print(mensaje_error)
            else:
                break
        resultado = n_esimo_peculiar(numero)
        print(f"El {numero}-ésimo peculiar es el {resultado}.") 
    elif opcion_seleccionada == 'E':
        # Cantidad de peculiares entre [n,m]
         while True:
             numero_n, mensaje_error_n = solicitar_entero("Ingrese el número entero n: ")
             if mensaje_error_n is not None:
                 print(mensaje_error_n)
             else:
                 break
         while True:
             numero_m, mensaje_error_m = solicitar_entero("Ingrese el número entero m: ")
             if mensaje_error_m is not None:
                 print(mensaje_error_m)
             else:
                 break
         resultado = cant_peculiares_entre(numero_n, numero_m)
         print(f"Entre {numero_n} y el {numero_m} hay {resultado} pecualiares") 
    elif opcion_seleccionada == 'F':
        finalizar = True
    else:
        print('Opción inválida.')

    if opcion_seleccionada != 'F':
        input('Presione ENTER para continuar.')
