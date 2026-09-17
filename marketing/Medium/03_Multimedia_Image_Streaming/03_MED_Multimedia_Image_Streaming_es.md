# Kafka no tiene opinión sobre tu imagen. Ese es el punto.

La mayoría de tutoriales de Kafka se detienen en los strings, porque el cliente ignora con educación la parte difícil: lo binario no es un tipo de payload, es una negociación (encoding, canales, frames, dimensiones). La postura de WKafka: el streaming de imágenes debería ser un flag `format`, no un proyecto de fin de semana.

> Día 03 de la serie open-source de WKafka — basado en decoradores, MIT, reproducible.

## Qué hace WKafka con una imagen

Serialización nativa `format="image"`. Lado productor:

```python
with kafka.producer() as p:
    p.send(
        topic="image_topic",
        value=frame,          # np.ndarray de OpenCV/NumPy
        format="image",
    )
```

El lado consumidor devuelve los bytes a un array utilizable sin tocar ni una llamada de decode:

```python
@kafka.consumer(topic="image_topic", format="image")
def on_image(image):
    print(f"Imagen recibida: shape={image.shape}")
    # ya es un np.ndarray manipulable (OpenCV/NumPy/PIL compatible)
```

El deserializador no es "compatible con JSON" — maneja **arrays dimensionales**: shape, dtype, orden de canales — metadatos que un blob de `bytes` crudo forzaría a tu consumidor a reconstruir a mano.

## Por qué importa

Videovigilancia, frames de satélite, estados de pizarra, detección de defectos en línea de producción. En cualquier lugar donde el mensaje es un *frame*, no un *hecho* — el orden de los bytes no significa nada; el orden de los frames es el producto.

## El resultado

> 🎯 En WKafka, una imagen es un ciudadano de primera clase en Kafka: mismo decorador, mismo `format`, mismo apagado. El ejemplo `03_multimedia` es un solo script ejecutable.

## Pruébalo tú mismo

- **PyPI:** https://pypi.org/project/wkafka
- **GitHub + `03_multimedia` ejecutable y reproducible:** https://github.com/wisrovi/wkafka
- **Docs:** https://wkafka.readthedocs.io

*¿Qué mensaje "no textual" sigue enviando tu equipo por Kafka como bytes crudos? Me encantaría ver esa lista.*

#Kafka #Python #ComputerVision #OpenCV #Streaming #OpenSource #Wisrovi