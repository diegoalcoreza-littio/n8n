"""Intercom conversation classifier - prompt v2.

Proposal for the data team's Gemini classifier (customer_success.intercom_conversations).
It applies CX's "Taxonomia de motivos de contacto CX - v1" (owner: Head of CX, 123 codes)
instead of the old product / request_type lists.

Fits the existing columns, no schema change required:
    product        <- taxonomy group           (e.g. "Recargas (dinero que entra)")
    request_type   <- nature of the reason     (Friccion / Intencion / Administrativo / Control)
    request_reason <- "CODE · Subcategory"     (e.g. "REC-01 · Recarga no reflejada / demorada")
Optional new columns: motivo_secundario, confianza, resumen.

The model only returns codes. Group and nature are looked up from the code here, so they can
never disagree with the taxonomy, and the prompt stays short.

Several conversations go in one call (BATCH_SIZE) to cut the number of Gemini requests.
"""

import json

TAXONOMY_VERSION = "v1"

# (code, group, subcategory, nature) - transcribed from the CX taxonomy v1 page.
TAXONOMY = [
    ("ONB-01", "Onboarding y verificación", "Cómo registrarse / abrir cuenta", "Intención"),
    ("ONB-02", "Onboarding y verificación", "Documentos aceptados (extranjeros, PPT, menores)", "Intención"),
    ("ONB-03", "Onboarding y verificación", "Verificación en proceso / tiempos", "Fricción"),
    ("ONB-04", "Onboarding y verificación", "Verificación rechazada", "Fricción"),
    ("ONB-05", "Onboarding y verificación", "Error al subir documentos o selfie", "Fricción"),
    ("ONB-06", "Onboarding y verificación", "Nueva verificación solicitada (link, actualización de KYC)", "Fricción"),
    ("ONB-07", "Onboarding y verificación", "País no disponible / lista de espera", "Intención"),
    ("CTA-01", "Mi cuenta y acceso", "No puede ingresar / iniciar sesión", "Fricción"),
    ("CTA-02", "Mi cuenta y acceso", "Código OTP o correo de acceso no llega", "Fricción"),
    ("CTA-03", "Mi cuenta y acceso", "PIN de acceso olvidado o bloqueado", "Fricción"),
    ("CTA-04", "Mi cuenta y acceso", "Cambio de dispositivo / biometría", "Fricción"),
    ("CTA-05", "Mi cuenta y acceso", "Cuenta bloqueada o desactivada", "Fricción"),
    ("CTA-06", "Mi cuenta y acceso", "Cambio de correo", "Administrativo"),
    ("CTA-07", "Mi cuenta y acceso", "Cambio de teléfono", "Administrativo"),
    ("CTA-08", "Mi cuenta y acceso", "Cambio de dirección", "Administrativo"),
    ("CTA-09", "Mi cuenta y acceso", "Cambio de nombre o documento", "Administrativo"),
    ("CTA-10", "Mi cuenta y acceso", "Cuenta inactiva / reactivación", "Fricción"),
    ("CTA-11", "Mi cuenta y acceso", "Eliminación de cuenta", "Administrativo"),
    ("CTA-12", "Mi cuenta y acceso", "Eliminación de datos personales", "Administrativo"),
    ("REC-01", "Recargas (dinero que entra)", "Recarga no reflejada / demorada", "Fricción"),
    ("REC-02", "Recargas (dinero que entra)", "Recarga en COP (PSE, Bre-B, Nequi, bancos)", "Intención"),
    ("REC-03", "Recargas (dinero que entra)", "Recarga desde USA (ACH, Wire, Zelle, PayPal, Deel)", "Intención"),
    ("REC-04", "Recargas (dinero que entra)", "Recarga desde Europa (IBAN / SEPA)", "Intención"),
    ("REC-05", "Recargas (dinero que entra)", "Recarga en México (CLABE / SPEI)", "Intención"),
    ("REC-06", "Recargas (dinero que entra)", "Recarga vía blockchain", "Intención"),
    ("REC-07", "Recargas (dinero que entra)", "Moneda o red equivocada en blockchain", "Fricción"),
    ("REC-08", "Recargas (dinero que entra)", "Recarga de terceros rechazada", "Fricción"),
    ("REC-09", "Recargas (dinero que entra)", "Límites y topes de recarga", "Intención"),
    ("REC-10", "Recargas (dinero que entra)", "Llaves Bre-B (crear, editar)", "Intención"),
    ("REC-11", "Recargas (dinero que entra)", "Recarga en efectivo", "Intención"),
    ("RET-01", "Retiros y transferencias (dinero que sale)", "Retiro demorado / en proceso", "Fricción"),
    ("RET-02", "Retiros y transferencias (dinero que sale)", "Retiro rechazado, cancelado o devuelto", "Fricción"),
    ("RET-03", "Retiros y transferencias (dinero que sale)", "Rastreo / comprobante de transferencia", "Fricción"),
    ("RET-04", "Retiros y transferencias (dinero que sale)", "Retiro a banco en Colombia", "Intención"),
    ("RET-05", "Retiros y transferencias (dinero que sale)", "Transferencia a USA (ACH, Wire)", "Intención"),
    ("RET-06", "Retiros y transferencias (dinero que sale)", "Transferencia a Europa", "Intención"),
    ("RET-07", "Retiros y transferencias (dinero que sale)", "Retiro en México (SPEI)", "Intención"),
    ("RET-08", "Retiros y transferencias (dinero que sale)", "Retiro a wallet blockchain", "Intención"),
    ("RET-09", "Retiros y transferencias (dinero que sale)", "Transferencia entre usuarios Littio (P2P)", "Intención"),
    ("RET-10", "Retiros y transferencias (dinero que sale)", "Pagos con QR Bre-B", "Intención"),
    ("RET-11", "Retiros y transferencias (dinero que sale)", "Retiro a cuenta de un tercero", "Intención"),
    ("RET-12", "Retiros y transferencias (dinero que sale)", "Límites de retiro", "Intención"),
    ("RET-13", "Retiros y transferencias (dinero que sale)", "Cancelar un retiro", "Fricción"),
    ("RET-14", "Retiros y transferencias (dinero que sale)", "Remesas a Venezuela", "Intención"),
    ("FX-01", "Cambio de divisas y tasas", "Cómo convertir entre monedas (USDC, EUROC, COPM, MXNT)", "Intención"),
    ("FX-02", "Cambio de divisas y tasas", "Tasa de cambio / diferencia con la TRM", "Intención"),
    ("FX-03", "Cambio de divisas y tasas", "Spread compra vs. venta", "Intención"),
    ("FX-04", "Cambio de divisas y tasas", "Mejora de tasa / tasa preferencial / Personal Trader", "Intención"),
    ("FX-05", "Cambio de divisas y tasas", "Conversión a la moneda equivocada", "Fricción"),
    ("CARD-01", "Littio Card", "Solicitar tarjeta virtual o física", "Intención"),
    ("CARD-02", "Littio Card", "Envío y entrega de la tarjeta física", "Fricción"),
    ("CARD-03", "Littio Card", "Activación de la tarjeta", "Fricción"),
    ("CARD-04", "Littio Card", "PIN de la tarjeta", "Fricción"),
    ("CARD-05", "Littio Card", "Tarjeta bloqueada, congelada o desactivada", "Fricción"),
    ("CARD-06", "Littio Card", "Compra rechazada", "Fricción"),
    ("CARD-07", "Littio Card", "Compra no reconocida", "Fricción"),
    ("CARD-08", "Littio Card", "Reembolso, devolución o cobro duplicado de comercio", "Fricción"),
    ("CARD-09", "Littio Card", "Suscripciones y pagos recurrentes", "Intención"),
    ("CARD-10", "Littio Card", "Apple Pay / Google Pay", "Intención"),
    ("CARD-11", "Littio Card", "Uso en el exterior y cajeros", "Intención"),
    ("CARD-12", "Littio Card", "Moneda de pago / completar con otras monedas", "Intención"),
    ("CARD-13", "Littio Card", "Límites y costos de la tarjeta", "Intención"),
    ("CARD-14", "Littio Card", "Reexpedición por pérdida o robo", "Fricción"),
    ("CARD-15", "Littio Card", "Términos y condiciones de la tarjeta", "Intención"),
    ("INV-01", "Inversiones", "Bóvedas: cómo funcionan y cómo crear", "Intención"),
    ("INV-02", "Inversiones", "Bóvedas: recompensas y plazos", "Intención"),
    ("INV-03", "Inversiones", "Bóvedas: recargar, retirar o renovar", "Intención"),
    ("INV-04", "Inversiones", "Bolsillos: cómo funcionan y cómo crear", "Intención"),
    ("INV-05", "Inversiones", "Bolsillos: mover dinero", "Intención"),
    ("INV-06", "Inversiones", "Recompensas no reflejadas (bóvedas, bolsillos, pots)", "Fricción"),
    ("INV-07", "Inversiones", "Oro digital", "Intención"),
    ("INV-08", "Inversiones", "Activos digitales (BTC, ETH)", "Intención"),
    ("INV-09", "Inversiones", "Acciones y ETFs", "Intención"),
    ("INV-10", "Inversiones", "Qué me conviene / Personal Banker", "Intención"),
    ("CINT-01", "Cuentas internacionales", "Cuenta USA: solicitud y activación", "Intención"),
    ("CINT-02", "Cuentas internacionales", "Cuenta USA: datos (routing, account, dirección, microdepósitos)", "Intención"),
    ("CINT-03", "Cuentas internacionales", "Cuenta Europa (IBAN): solicitud y datos", "Intención"),
    ("CINT-04", "Cuentas internacionales", "Cuenta México (CLABE): solicitud y datos", "Intención"),
    ("CINT-05", "Cuentas internacionales", "Error al solicitar la cuenta", "Fricción"),
    ("CINT-06", "Cuentas internacionales", "Cuenta desactivada por inactividad / reactivación", "Fricción"),
    ("CINT-07", "Cuentas internacionales", "Littio Global", "Intención"),
    ("PRO-01", "Littio Pro", "Beneficios y cómo funciona Littio Pro", "Intención"),
    ("PRO-02", "Littio Pro", "Activar o suscribirse a Pro", "Intención"),
    ("PRO-03", "Littio Pro", "Cancelar la membresía Pro", "Fricción"),
    ("PRO-04", "Littio Pro", "Cobros de la membresía Pro", "Fricción"),
    ("PRO-05", "Littio Pro", "Cashback y recompensas Pro", "Intención"),
    ("COST-01", "Comisiones y cobros", "Comisiones de la app", "Intención"),
    ("COST-02", "Comisiones y cobros", "Cobro por inactividad", "Fricción"),
    ("COST-03", "Comisiones y cobros", "Cobro no entendido / reembolso de un cobro", "Fricción"),
    ("DOC-01", "Certificados, extractos e impuestos", "Certificado tributario", "Administrativo"),
    ("DOC-02", "Certificados, extractos e impuestos", "Certificado de saldo, titularidad o cuenta", "Administrativo"),
    ("DOC-03", "Certificados, extractos e impuestos", "Extractos e historial de movimientos", "Administrativo"),
    ("DOC-04", "Certificados, extractos e impuestos", "DIAN / reporte de información", "Administrativo"),
    ("DOC-05", "Certificados, extractos e impuestos", "Implicaciones tributarias o migratorias", "Intención"),
    ("PROMO-01", "Campañas y referidos", "Campañas activas y condiciones", "Intención"),
    ("PROMO-02", "Campañas y referidos", "Gift o premio no recibido", "Fricción"),
    ("PROMO-03", "Campañas y referidos", "Referidos: cómo funciona / código", "Intención"),
    ("PROMO-04", "Campañas y referidos", "Recompensa por referido no pagada", "Fricción"),
    ("SEG-01", "Seguridad y fraude", "Robo o pérdida del celular", "Fricción"),
    ("SEG-02", "Seguridad y fraude", "Hackeo / acceso no autorizado", "Fricción"),
    ("SEG-03", "Seguridad y fraude", "Estafa, phishing o suplantación", "Fricción"),
    ("SEG-04", "Seguridad y fraude", "Movimiento no reconocido (fuera de la tarjeta)", "Fricción"),
    ("SEG-05", "Seguridad y fraude", "¿Es seguro Littio? Regulación y respaldo de fondos", "Intención"),
    ("CMP-01", "Compliance", "Solicitud de origen de fondos / documentos de OPS", "Administrativo"),
    ("CMP-02", "Compliance", "Cuenta restringida por compliance", "Administrativo"),
    ("CMP-03", "Compliance", "PEP / listas", "Administrativo"),
    ("CMP-04", "Compliance", "Fallecimiento del titular", "Administrativo"),
    ("APP-01", "App y Selenio", "Error o falla técnica", "Fricción"),
    ("APP-02", "App y Selenio", "Actualizar la app / versión", "Fricción"),
    ("APP-03", "App y Selenio", "Cómo usar Selenio / transacciones por WhatsApp", "Intención"),
    ("APP-04", "App y Selenio", "Sugerencia o nueva funcionalidad", "Intención"),
    ("APP-05", "App y Selenio", "Queja o reclamo sobre el servicio", "Fricción"),
    ("INFO-01", "Info general y prospectos", "Qué es Littio / cómo funciona", "Intención"),
    ("INFO-02", "Info general y prospectos", "Prospecto que aún no se registra", "Intención"),
    ("INFO-03", "Info general y prospectos", "Países donde opera", "Intención"),
    ("INFO-04", "Info general y prospectos", "Cuenta para empresas", "Intención"),
    ("INFO-05", "Info general y prospectos", "Horarios y canales de atención", "Intención"),
    ("INFO-06", "Info general y prospectos", "Consulta sobre la cuenta de un tercero", "Intención"),
    ("CTRL-01", "Control (no cuenta en métricas)", "Solo saludo / abandono", "Control"),
    ("CTRL-02", "Control (no cuenta en métricas)", "Pide asesor sin decir el motivo", "Control"),
    ("CTRL-03", "Control (no cuenta en métricas)", "Chat duplicado", "Control"),
    ("CTRL-04", "Control (no cuenta en métricas)", "Outbound / interno", "Control"),
    ("CTRL-05", "Control (no cuenta en métricas)", "Spam / no relacionado con Littio", "Control"),
]

BY_CODE = {code: (group, sub, nature) for code, group, sub, nature in TAXONOMY}
CODES = list(BY_CODE)

# Conversations per Gemini call. Tune against the account's request and token limits.
BATCH_SIZE = 20
# Keep each conversation bounded so a batch fits comfortably in the context window.
MAX_CHARS_PER_CONVERSATION = 6000


def _taxonomy_block():
    lines, current = [], None
    for code, group, sub, _ in TAXONOMY:
        if group != current:
            lines.append(f"\n{group}:")
            current = group
        lines.append(f"  {code} · {sub}")
    return "\n".join(lines).strip()


PROMPT_TEMPLATE = """Eres un analista del equipo de Customer Experience de Littio (Colombia).
Vas a clasificar conversaciones de soporte de Intercom según la taxonomía oficial de motivos de contacto de CX ({version}).

## Taxonomía (usa SOLO estos códigos)
{taxonomy}

## Reglas
1. Elige UN motivo principal por conversación: lo primero sustantivo que el cliente pide o reporta. Puedes dar un motivo secundario solo si la conversación trae claramente otro tema distinto; si no, null.
2. Ignora los textos de menú del bot Selenio ("Tengo preguntas sobre Littio", "Tengo una duda sobre un producto", "Consultas generales") y los mensajes automáticos. Clasifica por lo que escribe el cliente.
3. Compra no reconocida con Littio Card -> CARD-07. Cualquier otro movimiento no reconocido -> SEG-04.
4. Recargas (REC) = dinero que entra a Littio. Retiros (RET) = dinero que sale. Si no se sabe la dirección del dinero -> RET-03.
5. Si pregunta cómo funciona un producto, usa el grupo de ese producto, no INFO. INFO solo para preguntas generales sobre Littio.
6. Si el cliente responde a una solicitud de Compliance u OPS (origen de fondos, documentos) -> CMP-01, aunque hable de otro tema.
7. Si no hay un motivo sustantivo del cliente (solo saluda, abandona, pide asesor sin decir para qué, es outbound, spam) -> el código CTRL que corresponda.
8. Los tags de proceso de Intercom (A7, A9, O03, etc.) no son motivos: no los uses para decidir.
9. "confianza": "alta" si el motivo es explícito, "media" si lo infieres, "baja" si la conversación es ambigua o incompleta.
10. "resumen": una frase de máximo 180 caracteres con el motivo concreto, en español. No incluyas nombres, correos, teléfonos, números de documento ni de cuenta.

## Formato de respuesta
Devuelve SOLO un arreglo JSON, con un objeto por conversación y en el mismo orden, así:
[{{"id": "<id de la conversación>", "motivo": "<código>", "motivo_secundario": "<código o null>", "confianza": "alta|media|baja", "resumen": "<texto>"}}]

## Conversaciones
{conversations}
"""


def build_prompt(conversations):
    """conversations: list of (conversation_id, plain_text). Text is truncated per conversation."""
    blocks = [
        f'<conversacion id="{cid}">\n{(text or "")[:MAX_CHARS_PER_CONVERSATION]}\n</conversacion>'
        for cid, text in conversations
    ]
    return PROMPT_TEMPLATE.format(
        version=TAXONOMY_VERSION, taxonomy=_taxonomy_block(), conversations="\n\n".join(blocks)
    )


# Pass as generation_config={"response_mime_type": "application/json", "response_schema": RESPONSE_SCHEMA}
# so Gemini can only return valid codes.
RESPONSE_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "id": {"type": "STRING"},
            "motivo": {"type": "STRING", "enum": CODES},
            "motivo_secundario": {"type": "STRING", "enum": CODES, "nullable": True},
            "confianza": {"type": "STRING", "enum": ["alta", "media", "baja"]},
            "resumen": {"type": "STRING"},
        },
        "required": ["id", "motivo", "confianza", "resumen"],
    },
}


def to_rows(response_text, expected_ids):
    """Turn Gemini's JSON into rows for the existing table. Anything invalid or missing is
    marked for review instead of being written with a guessed value."""
    try:
        items = json.loads(response_text)
    except (TypeError, ValueError):
        items = []
    by_id = {str(i.get("id")): i for i in items if isinstance(i, dict)}

    rows = []
    for cid in expected_ids:
        item = by_id.get(str(cid))
        code = item.get("motivo") if item else None
        if code not in BY_CODE:
            rows.append({"id": cid, "product": None, "request_type": None, "request_reason": None,
                         "motivo_secundario": None, "confianza": None, "resumen": None,
                         "needs_review": True})
            continue
        group, sub, nature = BY_CODE[code]
        secondary = item.get("motivo_secundario")
        rows.append({
            "id": cid,
            "product": group,
            "request_type": nature,
            "request_reason": f"{code} · {sub}",
            "motivo_secundario": secondary if secondary in BY_CODE else None,
            "confianza": item.get("confianza"),
            "resumen": (item.get("resumen") or "")[:180],
            # CTRL-REVIEW in the CX doc: low-confidence conversations go to a human.
            "needs_review": item.get("confianza") == "baja",
        })
    return rows
