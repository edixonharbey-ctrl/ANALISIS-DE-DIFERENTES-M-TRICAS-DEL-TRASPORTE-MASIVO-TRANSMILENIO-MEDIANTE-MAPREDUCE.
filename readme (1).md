# Análisis de Métricas del Transporte Masivo TransMilenio mediante MapReduce

Repositorio académico enfocado en el desarrollo, simulación y ejecución de un pipeline analítico utilizando el paradigma de procesamiento distribuido **MapReduce** (desarrollado en Python puro con Pandas) para procesar un conjunto de datos transaccionales del sistema de transporte masivo TransMilenio (Bogotá).

---

## 📋 Descripción del Proyecto

Este proyecto simula el comportamiento de clústeres distribuidos (como Apache Hadoop YARN o Apache Spark) en un entorno accesible (Google Colab / Python puro). Su objetivo principal es procesar un dataset de **50,000 registros transaccionales**, extrayendo métricas clave relacionadas con:
1. **Recaudo y volumetría de pasajeros** por estación geográfica.
2. **Participación de mercado y recaudo** según el medio de pago (tipos de tarjeta).
3. **Comportamiento temporal y demanda operativa** por franjas horarias (identificación de horas pico de congestión).

---

## 🛠️ Arquitectura y Metodología MapReduce

El pipeline de procesamiento se estructura en tres fases fundamentales:
* **Fase MAP:** Toma cada registro transaccional de manera independiente y emite un par `(Clave, Valor)` compuesto por la dimensión de análisis (ej. Estación, Tipo de Tarjeta u Hora) y una tupla métrica `(1, monto_pasaje)`.
* **Fase SHUFFLE & SORT:** Agrupa de forma estructurada los valores emitidos por el Map de acuerdo con su clave única utilizando diccionarios optimizados (`defaultdict`), emulando la fase de red y ordenamiento de un clúster distribuido.
* **Fase REDUCE:** Procesa la lista de valores asociados a cada clave para consolidar las sumatorias totales de pasajeros y el recaudo financiero acumulado.

---

## 📂 Estructura del Dataset

El conjunto de datos (`transacciones_transmilenio.csv`) contiene variables transaccionales tales como:
* `id_transaccion`: Identificador único de la transacción (ej. `TRX-000001`).
* `fecha_hora`: Estampa de tiempo de la validación.
* `estacion`: Nombre de la estación de TransMilenio.
* `ruta`: Ruta del servicio de transporte.
* `tipo_tarjeta`: Medio de pago utilizado (`Tullave Plus`, `Tullave Basica`, `Credito/Debito`).
* `monto_pasaje`: Costo de la tarifa aplicada.
* `tiempo_val_segundos`: Tiempo de validación en segundos.

---

## 🚀 Paso a Paso de la Implementación

### 1. Carga y Exploración del Dataset
```python
import pandas as pd

# Cargar el dataset transaccional
df = pd.read_csv('transacciones_transmilenio.csv')
print("Primeras filas del dataset:")
display(df.head())
print(f"\nTotal de registros a procesar: {len(df)}")
```

### 2. Definición de la Función Mapper
```python
def mapper(row):
    """
    Función Map:
    Entrada: Una fila del DataFrame.
    Salida: Tupla (Clave, Valor) -> (Estacion, (1, monto))
    """
    estacion = row['estacion']
    monto = row['monto_pasaje']
    return (estacion, (1, monto))
```

### 3. Definición de Shuffle & Sort
```python
from collections import defaultdict

def shuffle_and_sort(mapped_data):
    """Agrupa los valores emitidos por el Map de acuerdo a su clave (Estación)."""
    grouped_data = defaultdict(list)
    for key, value in mapped_data:
        grouped_data[key].append(value)
    return grouped_data
```

### 4. Definición de la Función Reducer
```python
def reducer(estacion, lista_valores):
    """
    Función Reduce:
    Entrada: Clave (Estacion) y lista de valores [(1, monto1), (1, monto2), ...]
    Salida: Resultado agregado por estación.
    """
    total_pasajeros = 0
    recaudo_total = 0

    for pasajeros, monto in lista_valores:
        total_pasajeros += pasajeros
        recaudo_total += monto

    return {
        'estacion': estacion,
        'total_pasajeros': total_pasajeros,
        'recaudo_total': recaudo_total
    }
```

### 5. Ejecución del Pipeline Completo
```python
# Aplicar fases del MapReduce para Estaciones
mapped_results = [mapper(row) for _, row in df.iterrows()]
grouped_results = shuffle_and_sort(mapped_results)

final_results = []
for estacion, valores in grouped_results.items():
    resultado_reducido = reducer(estacion, valores)
    final_results.append(resultado_reducido)

df_resultado = pd.DataFrame(final_results)
df_resultado = df_resultado.sort_values(by='recaudo_total', ascending=False).reset_index(drop=True)
display(df_resultado)
```

---

## 📊 Resultados Analíticos Principales

### 1. Recaudo y Pasajeros por Estación
| Estación | Total Pasajeros | Recaudo Total ($) |
| :--- | :---: | :---: |
| **San Victorino** | 6,339 | 18,981,750 |
| **Ricaurte** | 6,303 | 18,886,950 |
| **Portal Norte** | 6,297 | 18,865,200 |
| **Portal Sur** | 6,267 | 18,768,900 |
| **Universidades** | 6,237 | 18,693,300 |
| **Portal 80** | 6,210 | 18,600,150 |
| **Calle 100** | 6,190 | 18,550,900 |
| **Banderas** | 6,157 | 18,431,650 |

### 2. Análisis por Tipo de Tarjeta (Medio de Pago)
| Tipo de Tarjeta | Total Pasajeros | Recaudo Total ($) |
| :--- | :---: | :---: |
| **Tullave Plus** | 25,046 | 75,019,850 |
| **Tullave Básica** | 14,933 | 44,742,350 |
| **Crédito / Débito** | 10,021 | 30,016,600 |

---

## 📈 Visualización Gráfica

El proyecto incluye la integración con librerías gráficas como `Matplotlib` y `Seaborn` para la representación visual del recaudo acumulado por estación y las curvas de tendencia de la demanda de pasajeros por hora del día.

```python
import matplotlib.pyplot as plt
import seaborn as sns

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Gráfico 1: Recaudo por Estación
sns.barplot(data=df_resultado, x='recaudo_total', y='estacion', ax=axes[0], palette='Blues_r', hue='estacion', legend=False)
axes[0].set_title('Recaudo Total por Estación')
axes[0].set_xlabel('Recaudo ($)')
axes[0].set_ylabel('Estación')

# Gráfico 2: Pasajeros por Hora
sns.lineplot(data=df_horas, x='hora', y='total_pasajeros', marker='o', ax=axes[1], color='orange', linewidth=2.5)
axes[1].set_title('Demanda de Pasajeros por Hora del Día')
axes[1].set_xlabel('Hora')
axes[1].set_ylabel('Total Pasajeros')
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()
```

---

## 🔍 Conclusiones y Escalabilidad

* **Distribución de Carga:** El procesamiento de los 50,000 registros muestra una distribución homogénea en estaciones gracias a la aleatoriedad controlada, simulando con éxito el balanceo de carga de Hadoop YARN.
* **Medios de Pago:** Se evidencia una alta adopción de la modalidad *Tullave Plus*, permitiendo enfocar estrategias tecnológicas o campañas de recarga digital en los métodos de mayor impacto.
* **Comportamiento Temporal:** La agregación horaria facilita la detección de picos de congestión, datos fundamentales para dimensionar flotas de buses de refuerzo o mantenimientos programados.
* **Extensibilidad:** La lógica MapReduce implementada es altamente extensible; modificando la función `mapper` y las claves de agrupación, es posible migrar estos algoritmos a entornos de Big Data reales con **PySpark** o **Hadoop Streaming** sin alterar la lógica de negocio central.

---

## 🔗 Enlaces de Interés
* **Cuaderno Interactivo:** [Google Colab Notebook](#) *(Actualizar enlace)*
* **Autor:** Edixon Harbey Rubiano Robayo
* **Institución:** Universidad de Cundinamarca – Analítica y Ciencia de Datos (Big Data)