Un frame de cámara no es "bytes que pasan por suerte": es un mensaje con shape, dtype y canales. Con WKafka el tipo viaja en `format="image"` — el serializador que codifica es el que decodifica, round-trip determinístico OpenCV/NumPy/PIL, orden por clave preservado. Hoy: streaming multimedia, un decorador, código reproducible (`03_multimedia`).

- PyPI: https://pypi.org/project/wkafka
- GitHub: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*¿Qué dato no textual sigues mandando por Kafka como bytes crudos? Te leo en comentarios.*