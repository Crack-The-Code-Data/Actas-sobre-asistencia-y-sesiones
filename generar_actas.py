import pandas as pd
import athena_utils as au
import locale
import os
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from jinja2 import Template
from xhtml2pdf import pisa
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from dotenv import load_dotenv

locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')


def _build_query_alumnos(proyecto):
    return f"""
    WITH 
    -- ============================================== 
    -- filtros de fecha 
    -- ============================================== 
    parametros AS ( 
        SELECT 
            DATE '2021-01-01' AS fecha_inicio, --just in case 
            CURRENT_DATE AS fecha_fin -- ídem 
    )
    -- ============================================== 
    -- Alumnos por salon
    -- ============================================== 
        SELECT distinct
            p.id as id,
            g.institution as institucion,
            g.grade as grado,
            c.id as room_id,
            g.student_id
        FROM datalake.room_roomsessions s 
        CROSS JOIN parametros 
        JOIN datalake.room_room_students b ON s.room_id = b.room_id 
        JOIN datalake.student_student f ON b.student_id = f.id 
        LEFT JOIN datalake.room_room c ON s.room_id = c.id 
        LEFT JOIN datalake.learning_group d ON c.group_id = d.id 
        LEFT JOIN datalake.learning_course e ON d.course_id = e.id 
        LEFT JOIN datalake.enrollment_enrolment g ON f.id = g.student_id AND e.id = g.course_id AND d.id = g.group_id 
        LEFT JOIN datalake.projects p ON g.b2b_project_id = p.id 
        LEFT JOIN datalake.attendance_attendance aa ON aa.room_session_id = s.id AND b.student_id = aa.object_id AND aa.content_type_id = 8 
        WHERE s.start_date BETWEEN parametros.fecha_inicio AND parametros.fecha_fin 
            AND COALESCE(g.state, 'active') NOT IN ('inactive', 'abandoned') 
            AND ( 
                (s.state = 'true' AND aa.status IN (1,2,3,4,5,6)) 
                OR (s.state = 'false' AND s.cancellation_reason_id IN (34,35)) 
            )
            and p.id in ({proyecto})
    """


def _build_query_cancelaciones(proyecto, fecha_inicio, fecha_fin):
    return f"""
WITH
cte_Organization AS (
    SELECT
        poa.project_id,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT o.name), ', ') AS org_names
    FROM datalake.project_organization_association poa
    JOIN datalake.organizations o ON poa.organization_id = o.id
    GROUP BY poa.project_id
),
cte_ProgramType AS (
    SELECT
        ppta.project_id,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT cbpt.name), ', ') AS program_types
    FROM datalake.project_program_type_association ppta
    JOIN datalake.catalog_b2bprogramtype cbpt ON ppta.program_type_id = cbpt.id
    GROUP BY ppta.project_id
),
base AS (
    SELECT
        DISTINCT
        p.id AS projectsID,
        p.name AS "Proyecto",
        p.internal_name,
        (CASE WHEN ee.institution IS NULL THEN ei.name ELSE ee.institution END) AS institucion,
        ei.municipality as Municipio,
        rr.id AS room,
        rs.id AS sesionID,
        rs.session_number AS sesion,
        ee.grade AS grado,
        (CASE WHEN rs.cancellation_reason_id IS NULL THEN 36 ELSE rs.cancellation_reason_id END) AS reasonID,
        (CASE WHEN rc.name IS NULL THEN 'N/A' ELSE rc.name END) AS Motivo,
        rs.start_date AS "Fecha",
        rs.state AS state,
        
        CONCAT(rraux.name, ' ', rraux.last_name) AS profesor_ied_nombre,
        CASE 
            WHEN aa_ied.status IN (1, 5) THEN true
            ELSE false
        END AS profesor_ied_asistio,
        'boattiniad@gmail.com' AS email_responsable,
        'jacinto' as profesor_responsable

    FROM
        datalake.room_roomsessions rs
        LEFT JOIN datalake.room_room rr ON rs.room_id = rr.id
        JOIN datalake.enrollment_enrolment ee ON rr.id = ee.room_id AND ee.b2b_project_id IS NOT NULL
        LEFT JOIN datalake.projects p ON (ee.b2b_project_id = p.id OR p.id = rr.project_b2b_id)
        LEFT JOIN cte_Organization o ON p.id = o.project_id
        LEFT JOIN cte_ProgramType pt ON p.id = pt.project_id
        LEFT JOIN datalake.account_user au ON rr.teacher_id = au.id
        LEFT JOIN datalake.catalog_reasonsessioncancellation rc ON (
            (CASE WHEN (rs.state = 'false' AND rs.cancellation_reason_id IS NULL) THEN 36 ELSE rs.cancellation_reason_id END) = rc.id
        )
        LEFT JOIN datalake.educational_institution ei ON ei.id = rr.educational_institution_id
        -- Join para asistencia del profesor IED
        LEFT JOIN datalake.attendance_attendance aa_ied 
            ON aa_ied.room_session_id = rs.id 
            AND aa_ied.room_id = rr.id 
            AND aa_ied.content_type_id = 276
        LEFT JOIN datalake.room_roomauxiliarteacher rraux 
            ON rraux.id = aa_ied.object_id

    WHERE p.id = {proyecto}
)
SELECT *
FROM base
WHERE
    NOT (state = 'false' AND projectsID IN (19, 14) AND (("Fecha" BETWEEN DATE '2024-06-17' AND DATE '2024-07-05') OR ("Fecha" BETWEEN DATE '2024-10-07' AND DATE '2024-10-14')))
    AND
    NOT (state = 'false' AND projectsID IN (47, 48, 56) AND ("Fecha" BETWEEN DATE '2024-10-07' AND DATE '2024-10-14'))
    AND
    (reasonID NOT IN (33, 34, 35) OR reasonID IS NULL)
    AND "Fecha" BETWEEN DATE '{fecha_inicio}' AND DATE '{fecha_fin}';
    """


def _ejecutar_queries_paralelo(queries_dict, max_workers=5):
    results = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(au.run_athena_query_auto, query, name): name
            for name, query in queries_dict.items()
        }
        for future in as_completed(futures):
            query_name = futures[future]
            try:
                df = future.result()
                results[query_name] = df
                print(f"  {query_name}: {len(df)} filas obtenidas")
            except Exception as e:
                errors[query_name] = str(e)
                print(f"  {query_name}: Error - {e}")
    if errors:
        print(f"  Queries con error: {list(errors.keys())}")
    return results


def _calcular_mes_ano(df_cancelaciones):
    mes_ano_counts = df_cancelaciones['mes_año'].value_counts()
    total_count = mes_ano_counts.sum()
    max_count = mes_ano_counts.max()
    if max_count / total_count >= 0.8:
        return mes_ano_counts.index[0]
    top_two = mes_ano_counts.head(2)
    top_two = top_two.sort_index(
        key=lambda x: pd.to_datetime(x, format='%B de %Y')
    )
    return f"{top_two.index[0]} y {top_two.index[1]}"


def _calcular_metricas(df_cancelaciones, df_estudiantes):
    instituciones = df_cancelaciones['institucion'].dropna().unique()
    metricas = {}
    for inst in instituciones:
        dc = df_cancelaciones[df_cancelaciones['institucion'] == inst]
        de = df_estudiantes[df_estudiantes['institucion'] == inst]
        sesiones_prog = dc['sesionid'].nunique()
        sesiones_ejec = dc[dc['state'] == 'true']['sesionid'].nunique()
        sesiones_dict = round(sesiones_ejec / sesiones_prog, 3) if sesiones_prog > 0 else 0
        sesiones_asist = dc[
            (dc['state'] == 'true') & (dc['profesor_ied_asistio'] == True)
        ]['sesionid'].nunique()
        porc_asist = round(sesiones_asist / sesiones_ejec, 3) if sesiones_ejec > 0 else 0
        estudiantes = de['student_id'].nunique()
        cant_salones = de['room_id'].nunique()
        grados_list = sorted(de['grado'].astype(int).unique())
        grados = ', '.join(f"{g}\u00b0" for g in grados_list[:-1]) + f" y {grados_list[-1]}\u00b0"
        profesor_responsable = dc['profesor_responsable'].iloc[0] if not dc.empty else 'N/A'
        metricas[inst] = {
            'sesiones_programadas': sesiones_prog,
            'sesiones_ejecutadas': sesiones_ejec,
            'sesiones_dictadas': sesiones_dict,
            'sesiones_asistidas': sesiones_asist,
            'porcentaje_asistencia': porc_asist,
            'estudiantes': estudiantes,
            'cantidad_salones': cant_salones,
            'grados': grados,
            'profesor_responsable': profesor_responsable
        }
    return metricas


def _enviar_email(gmail_user, gmail_password, destinatario, asunto, cuerpo, archivo_adjunto):
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = destinatario
    msg['Subject'] = asunto
    msg.attach(MIMEText(cuerpo, 'plain', 'utf-8'))
    nombre = os.path.basename(archivo_adjunto)
    with open(archivo_adjunto, 'rb') as f:
        adjunto = MIMEApplication(f.read(), _subtype='pdf')
    adjunto.add_header('Content-Disposition', 'attachment',
                       filename=('utf-8', '', nombre))
    msg.attach(adjunto)
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.send_message(msg)


def generar_y_enviar(proyecto, convenio, fecha_inicio, fecha_fin,
                     plantilla_path='plantilla.html',
                     output_folder='actas_generadas',
                     enviar_emails=True):
    """
    Genera informes PDF por institucion y opcionalmente los envia por email.

    Args:
        proyecto: ID del proyecto (int).
        convenio: Codigo del convenio (str).
        fecha_inicio: Fecha inicio en formato 'YYYY-MM-DD'.
        fecha_fin: Fecha fin en formato 'YYYY-MM-DD'.
        plantilla_path: Ruta a la plantilla HTML (default: 'plantilla.html').
        output_folder: Carpeta de salida para los PDFs (default: 'actas_generadas').
        enviar_emails: Si True, envia los PDFs por email (default: True).
    """
    # --- 1. Queries ---
    print("Ejecutando queries...")
    queries = {
        'cancelaciones': _build_query_cancelaciones(proyecto, fecha_inicio, fecha_fin),
        'estudiantes': _build_query_alumnos(proyecto),
    }
    dataframes = _ejecutar_queries_paralelo(queries)
    df_cancelaciones = dataframes['cancelaciones']
    df_estudiantes = dataframes['estudiantes']

    # --- 2. Preparar datos ---
    df_cancelaciones['fecha'] = pd.to_datetime(df_cancelaciones['fecha'])
    df_cancelaciones['mes_año'] = df_cancelaciones['fecha'].dt.strftime('%B de %Y')

    hoy = pd.Timestamp.now().normalize()
    dia_de_hoy = hoy.strftime('%d de %B de %Y')
    mes_ano = _calcular_mes_ano(df_cancelaciones)
    metricas_por_institucion = _calcular_metricas(df_cancelaciones, df_estudiantes)

    # --- 3. Generar PDFs ---
    os.makedirs(output_folder, exist_ok=True)
    with open(plantilla_path, 'r', encoding='utf-8') as f:
        template = Template(f.read())

    logo_ctc = os.path.abspath('logo_ctc.png')

    print(f"\nGenerando PDFs en '{output_folder}'...")
    for institucion, metricas in metricas_por_institucion.items():
        variables = {
            'ied': institucion,
            'sesiones_programadas': metricas['sesiones_programadas'],
            'sesiones_ejecutadas': metricas['sesiones_ejecutadas'],
            'sesiones_dictadas': f"{metricas['sesiones_dictadas']*100:.1f}%",
            'sesiones_asistidas': metricas['sesiones_asistidas'],
            'porcentaje_asistencia': f"{metricas['porcentaje_asistencia']*100:.1f}%",
            'cantidad_estudiantes': metricas['estudiantes'],
            'cantidad_salones': metricas['cantidad_salones'],
            'dia_mes_ano': dia_de_hoy,
            'mes_ano': mes_ano,
            'convenio': convenio,
            'grados': metricas['grados'],
            'nombre_docente': metricas['profesor_responsable'],
            'logo_ctc': logo_ctc,
        }
        html_renderizado = template.render(**variables)
        nombre_archivo = f"{institucion.replace('/', '-').replace(chr(92), '-')}.pdf"
        ruta_salida = os.path.join(output_folder, nombre_archivo)
        with open(ruta_salida, 'wb') as f:
            status = pisa.CreatePDF(html_renderizado, dest=f)
        if status.err:
            print(f"  Error: {nombre_archivo}")
        else:
            print(f"  Generado: {nombre_archivo}")

    print(f"\nSe generaron {len(metricas_por_institucion)} documentos.")

    # --- 4. Enviar emails ---
    if not enviar_emails:
        return

    load_dotenv()
    gmail_user = os.environ['GMAIL_USER']
    gmail_password = os.environ['GMAIL_APP_PASSWORD']

    email_por_institucion = (
        df_cancelaciones
        .dropna(subset=['institucion', 'email_responsable'])
        .drop_duplicates(subset=['institucion'])
        .set_index('institucion')['email_responsable']
        .to_dict()
    )

    print("\nEnviando emails...")
    enviados = 0
    errores = []

    for institucion in metricas_por_institucion:
        email = email_por_institucion.get(institucion)
        if not email:
            errores.append(f"{institucion}: sin email_responsable")
            continue

        nombre_archivo = f"{institucion.replace('/', '-').replace(chr(92), '-')}.pdf"
        ruta_archivo = os.path.join(output_folder, nombre_archivo)
        if not os.path.exists(ruta_archivo):
            errores.append(f"{institucion}: archivo no encontrado")
            continue

        asunto = f"Informe {institucion} - {mes_ano}"
        cuerpo = (
            f"Cordial saludo,\n\n"
            f"Adjunto encontrara el informe de asistencia y sesiones de {institucion} "
            f"correspondiente a {mes_ano}.\n\n"
            f"Cualquier inquietud no dude en comunicarse.\n\n"
            f"Atentamente."
        )

        try:
            _enviar_email(gmail_user, gmail_password, email, asunto, cuerpo, ruta_archivo)
            enviados += 1
            print(f"  Enviado: {institucion} -> {email}")
        except Exception as e:
            errores.append(f"{institucion} ({email}): {e}")
            print(f"  Error: {institucion} -> {e}")

    print(f"\nResumen: {enviados}/{len(metricas_por_institucion)} emails enviados")
    if errores:
        print("Errores:")
        for err in errores:
            print(f"  - {err}")