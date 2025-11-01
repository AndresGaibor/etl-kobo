# Análisis de IDs en KoboToolbox

## IDs Principales (IMPORTANTES ⭐)

### `_id`
- **Tipo**: Integer
- **Ejemplo**: `504238666`
- **Descripción**: ID único numérico secuencial de la submission en Kobo
- **Importancia**: ⭐⭐⭐ **MUY IMPORTANTE**
- **Uso**: Identificador primario, útil para tracking y referencias
- **Recomendación**: **MANTENER** - Es el ID más simple y directo

### `_uuid`
- **Tipo**: String (UUID v4)
- **Ejemplo**: `fe833c3c-6324-4f6b-870b-841eba799f11`
- **Descripción**: UUID único global de la submission
- **Importancia**: ⭐⭐⭐ **MUY IMPORTANTE**
- **Uso**: Identificador universal, persiste aunque se exporte/importe
- **Recomendación**: **MANTENER** - Crucial para integraciones y sincronización

---

## IDs del Sistema (ÚTILES 📋)

### `_submission_time`
- **Tipo**: String (ISO 8601)
- **Ejemplo**: `2025-06-25T03:53:54`
- **Descripción**: Fecha y hora de envío de la encuesta
- **Importancia**: ⭐⭐ **IMPORTANTE**
- **Uso**: Análisis temporal, auditoría
- **Recomendación**: **MANTENER** - Útil para reportes

### `_status`
- **Tipo**: String
- **Ejemplo**: `submitted_via_web`
- **Descripción**: Estado/método de envío de la submission
- **Importancia**: ⭐ **ÚTIL**
- **Uso**: Conocer si fue enviado por web, app, etc.
- **Recomendación**: **MANTENER** - Puede ser útil para análisis

### `__version__`
- **Tipo**: String
- **Ejemplo**: `v6DsRHKibHt2Da2t45pVw6`
- **Descripción**: Versión del formulario con el que se envió
- **Importancia**: ⭐⭐ **IMPORTANTE**
- **Uso**: Rastrear cambios en el formulario a lo largo del tiempo
- **Recomendación**: **MANTENER** - Importante si el formulario cambia

---

## IDs Redundantes (ELIMINAR ❌)

### `meta/instanceID`
- **Tipo**: String
- **Ejemplo**: `uuid:fe833c3c-6324-4f6b-870b-841eba799f11`
- **Descripción**: Mismo UUID pero con prefijo "uuid:"
- **Importancia**: ❌ **REDUNDANTE**
- **Razón**: Es exactamente `_uuid` con prefijo
- **Recomendación**: **ELIMINAR** - Ya tienes `_uuid`

### `meta/rootUuid`
- **Tipo**: String
- **Ejemplo**: `uuid:fe833c3c-6324-4f6b-870b-841eba799f11`
- **Descripción**: UUID raíz (igual al instanceID en submissions simples)
- **Importancia**: ❌ **REDUNDANTE** (en tu caso)
- **Razón**: Solo es diferente si hay formularios repetidos/anidados
- **Recomendación**: **ELIMINAR** - No usas formularios repetidos

### `formhub/uuid`
- **Tipo**: String
- **Ejemplo**: `e5ab5cc1f329473d97d2e5b962e3c381`
- **Descripción**: UUID del deployment del formulario
- **Importancia**: ❌ **NO NECESARIO**
- **Razón**: Es el mismo para TODAS las submissions del mismo formulario
- **Recomendación**: **ELIMINAR** - No cambia por submission

### `_xform_id_string`
- **Tipo**: String
- **Ejemplo**: `a7PzZkvgeHJkbCiYNz57Gi`
- **Descripción**: ASSET_UID del formulario
- **Importancia**: ❌ **NO NECESARIO**
- **Razón**: Es el mismo para TODAS las submissions, ya lo tienes en .env
- **Recomendación**: **ELIMINAR** - Es constante

---

## IDs Vacíos (ELIMINAR 🗑️)

### `_submitted_by`
- **Tipo**: NULL
- **Descripción**: Usuario que envió (vacío si es anónimo)
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - Está NULL en todas las submissions

### `_validation_status`
- **Tipo**: Dict vacío `{}`
- **Descripción**: Estado de validación
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - No se usa validación

### `_attachments`
- **Tipo**: Array vacío `[]`
- **Descripción**: Archivos adjuntos (fotos, documentos)
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - No hay attachments

### `_tags`
- **Tipo**: Array vacío `[]`
- **Descripción**: Etiquetas asignadas a la submission
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - No se usan tags

### `_notes`
- **Tipo**: Array vacío `[]`
- **Descripción**: Notas añadidas a la submission
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - No hay notas

### `_geolocation`
- **Tipo**: Array `[null, null]`
- **Descripción**: Coordenadas GPS de la submission
- **Importancia**: ❌ **VACÍO**
- **Recomendación**: **ELIMINAR** - No se captura ubicación

---

## Resumen de Recomendaciones

### ✅ MANTENER (5 campos)
1. `_id` - ID numérico principal
2. `_uuid` - UUID global único
3. `_submission_time` - Timestamp de envío
4. `_status` - Estado/método de envío
5. `__version__` - Versión del formulario

### ❌ ELIMINAR (10 campos)
1. `meta/instanceID` - Redundante con `_uuid`
2. `meta/rootUuid` - Redundante con `_uuid`
3. `formhub/uuid` - Constante, igual para todas
4. `_xform_id_string` - Constante, es el ASSET_UID
5. `_submitted_by` - Siempre NULL
6. `_validation_status` - Siempre `{}`
7. `_attachments` - Siempre `[]`
8. `_tags` - Siempre `[]`
9. `_notes` - Siempre `[]`
10. `_geolocation` - Siempre `[null, null]`

---

## Implementación

Para aplicar estas recomendaciones, el script `limpieza.py` ya detectará y eliminará automáticamente:
- Campos NULL (`_submitted_by`)
- Campos con `{}` (`_validation_status`)
- Campos con `[]` (`_attachments`, `_tags`, `_notes`)
- Campos con `[null, null]` (`_geolocation`)

Para los campos redundantes pero no vacíos (`meta/instanceID`, `meta/rootUuid`, `formhub/uuid`, `_xform_id_string`), puedes:
1. Dejarlos que `limpieza.py` los detecte como constantes (todos tienen el mismo valor)
2. O agregarlos manualmente a una lista de exclusión
