package eu.andreatt;

import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XQueryService;
import org.xmldb.api.base.ResourceSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GimnasioDB {

	// Configuración de conexión a eXistDB
	private static final String URI = "xmldb:exist://localhost:8080/exist/xmlrpc"; // URI para conectar a eXistDB
	private static final String USER = "admin"; // Usuario de eXistDB
	private static final String PASSWORD = ""; // Contraseña de eXistDB

	private static Collection conexion; // Objeto para manejar la conexión a la colección de eXistDB

	public static void main(String[] args) {
		try {
			// Conexión inicial con la base de datos
			conectar();

			// Crear la colección si no existe
			crearColeccionSiNoExiste("/db/GIMNASIO");

			// Menú interactivo
			while (true) {
				System.out.println("1. Cuota Socio");
				System.out.println("2. Cuota Total");
				System.out.println("3. Subir Ficheros");
				System.out.println("0. Salir");

				// Leer opción del usuario
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				String opcion = reader.readLine();

				if ("1".equals(opcion)) {
					cuotaSocio(); // Calcular la cuota adicional de actividades por socio
				} else if ("2".equals(opcion)) {
					cuotaTotal(); // Calcular la cuota total por socio
				} else if ("3".equals(opcion)) {
					subirFicheros(); // Subir los ficheros XML a la colección
				} else if ("0".equals(opcion)) {
					break; // Salir del programa
				}
			}

		} catch (Exception e) {
			e.printStackTrace(); // Manejar errores de conexión y ejecución
		} finally {
			desconectar(); // Cerrar la conexión
		}
	}

	/**
	 * Establece la conexión con eXistDB.
	 */
	private static void conectar() throws Exception {
		try {
			// Registrar el driver de eXistDB
			Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
			Database database = (Database) cl.getDeclaredConstructor().newInstance();
			database.setProperty("create-database", "true");
			DatabaseManager.registerDatabase(database);

			// Conectar a la colección raíz
			conexion = DatabaseManager.getCollection(URI + "/db", USER, PASSWORD);
			if (conexion == null) {
				throw new Exception("No se pudo conectar con eXistDB en " + URI);
			}
			System.out.println("Conexión establecida con eXistDB.");
		} catch (Exception e) {
			throw new Exception("Error al conectar con eXistDB: " + e.getMessage(), e);
		}
	}

	/**
	 * Cierra la conexión con eXistDB.
	 */
	private static void desconectar() {
		if (conexion != null) {
			try {
				conexion.close(); // Cerrar conexión con eXistDB
				System.out.println("Conexión cerrada.");
			} catch (XMLDBException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Crea una colección en eXistDB si no existe.
	 */
	private static void crearColeccionSiNoExiste(String path) throws Exception {
		try {
			// Verificar si la colección ya existe
			if (DatabaseManager.getCollection(URI + path, USER, PASSWORD) == null) {
				Collection root = DatabaseManager.getCollection(URI + "/db", USER, PASSWORD);
				// Obtener servicio de gestión de colecciones
				CollectionManagementService service = (CollectionManagementService) root
						.getService("CollectionManagementService", "1.0");
				// Crear colección
				service.createCollection(path.substring(path.lastIndexOf('/') + 1));
				System.out.println("Colección creada: " + path);
			} else {
				System.out.println("La colección ya existe: " + path);
			}
		} catch (XMLDBException e) {
			throw new Exception("Error al crear la colección: " + e.getMessage(), e);
		}
	}
	/**
	 * Sube los ficheros XML a la colección.
	 */
	private static void subirFicheros() throws IOException, XMLDBException {
		// Lista de ficheros a subir
		String[] files = { "actividades_gim.xml", "socios_gim.xml", "uso_gimnasio.xml" };
		for (String file : files) {
			// Leer el contenido del fichero
			String content = leerFichero("src/main/resources/ColeccionGimnasio/" + file);
			XQueryService service = (XQueryService) conexion.getService("XQueryService", "1.0");
			// Subir el contenido a la colección
			service.query("xmldb:store('/db/GIMNASIO', '" + file + "', '" + content.replace("'", "\\'") + "')");
			System.out.println("Archivo subido: " + file);
		}
	}

	/**
	 * Lee un archivo y devuelve su contenido como String.
	 */
	private static String leerFichero(String fileName) throws IOException {
		// Leer el contenido del archivo línea por línea
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line).append("\n");
		}
		br.close();
		return sb.toString();
	}

	/**
	 * Calcula la cuota adicional de cada actividad realizada por socio.
	 */
	private static void cuotaSocio() throws XMLDBException {
		// Consulta XQuery para calcular cuota adicional
		String query = "for $uso in doc('/db/GIMNASIO/uso_gimnasio.xml')/USO_GIMNASIO/fila_uso "
				+ "let $socio := doc('/db/GIMNASIO/socios_gim.xml')/SOCIOS_GIM/fila_socios[COD = $uso/CODSOCIO]/NOMBRE/text() "
				+ "let $actividad := doc('/db/GIMNASIO/actividades_gim.xml')/ACTIVIDADES_GIM/fila_actividades[@cod = $uso/CODACTIV] "
				+ "let $horas := xs:integer($uso/HORAFINAL) - xs:integer($uso/HORAINICIO) "
				+ "let $tipo_actividad := $actividad/@tipo/data() "
				+ "let $cuota_adicional := if ($tipo_actividad = 2) then $horas*2 else if ($tipo_actividad = 3) then $horas*4 else 0 "
				+ "return <datos><COD>{$uso/CODSOCIO/text()}</COD><NOMBRESOCIO>{$socio}</NOMBRESOCIO>"
				+ "<CODACTIV>{$uso/CODACTIV/text()}</CODACTIV><NOMBREACTIVIDAD>{$actividad/NOMBRE/text()}</NOMBREACTIVIDAD>"
				+ "<horas>{$horas}</horas><tipoact>{$tipo_actividad}</tipoact><cuota_adicional>{$cuota_adicional}</cuota_adicional></datos>";

		XQueryService service = (XQueryService) conexion.getService("XQueryService", "1.0");
		ResourceSet result = service.query(query);

		// Crear XML intermedio
		StringBuilder xmlFinal = new StringBuilder("<intermedio>");
		for (int i = 0; i < result.getSize(); i++) {
			xmlFinal.append(result.getResource(i).getContent());
		}
		xmlFinal.append("</intermedio>");

		// Guardar XML intermedio en la colección
		service.query(
				"xmldb:store('/db/GIMNASIO', 'intermedio.xml', '" + xmlFinal.toString().replace("'", "\\'") + "')");
		System.out.println("Archivo 'intermedio.xml' creado y almacenado.");
	}

	/**
	 * Calcula la cuota total por socio.
	 */
	private static void cuotaTotal() throws XMLDBException {
		// Consulta XQuery para calcular cuota total por socio
		String query = "for $socio in doc('/db/GIMNASIO/socios_gim.xml')/SOCIOS_GIM/fila_socios "
				+ "let $intermedio := doc('/db/GIMNASIO/intermedio.xml')/intermedio/datos "
				+ "let $codigoSocio := $socio/COD/text() " + "let $nombreSocio := $socio/NOMBRE/text() "
				+ "let $cuotaFija := $socio/CUOTA_FIJA/text() "
				+ "let $sumaCuotaAdicional := sum($intermedio[COD = $socio/COD]/cuota_adicional) "
				+ "let $cuotaTotal := $cuotaFija + $sumaCuotaAdicional "
				+ "return <datos><COD>{$codigoSocio}</COD><NOMBRESOCIO>{$nombreSocio}</NOMBRESOCIO>"
				+ "<CUOTA_FIJA>{$cuotaFija}</CUOTA_FIJA><suma_cuota_adic>{$sumaCuotaAdicional}</suma_cuota_adic>"
				+ "<cuota_total>{$cuotaTotal}</cuota_total></datos>";

		XQueryService service = (XQueryService) conexion.getService("XQueryService", "1.0");
		ResourceSet result = service.query(query);

		// Crear XML de cuota total
		StringBuilder xmlFinal = new StringBuilder("<cuota>");
		for (int i = 0; i < result.getSize(); i++) {
			xmlFinal.append(result.getResource(i).getContent());
		}
		xmlFinal.append("</cuota>");

		// Guardar XML de cuota total en la colección
		service.query(
				"xmldb:store('/db/GIMNASIO', 'cuota_total.xml', '" + xmlFinal.toString().replace("'", "\\'") + "')");
		System.out.println("Archivo 'cuota_total.xml' creado y almacenado.");
	}
}

