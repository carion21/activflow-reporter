
from constants import *
from utilities import *
from minioservice import MinioService


def treat_report(env_vars, jwt_token, report):
    """Treat report."""
    logger = env_vars.get("logger")
    print("-" * 50)
    logger.info(f"Traitement du rapport {report.get('code')} en cours...")
    report_data = None
    try:
        r_core_data = core_get_activity_data(env_vars, jwt_token, report)
        if r_core_data.get("success"):
            report_data = r_core_data.get("data")

            if len(report_data) == 0:
                print(
                    f"Aucune donnée trouvée pour le rapport {report.get('code')}")
                return

            minioservice = MinioService()

            output_filename = f"{report.get('code')}.xlsx"
            output_filepath = generate_activity_report(
                minioservice=minioservice,
                report_data=report_data,
                output_file=output_filename
            )

            if output_filepath:
                file = {
                    "filename": output_filename,
                    "stream": open(output_filepath, 'rb'),
                    "content_length": os.path.getsize(output_filepath),
                }

                print('Uploading file to Minio :', output_filename)
                minioservice = MinioService()
                minioReportFile = minioservice.upload_file(
                    file=file,
                    generate_object_url=True
                )

                print('Minio report file uploaded :',
                      minioReportFile.get('object_name'))

                report['filename'] = minioReportFile.get('object_name')
                report['filelink'] = minioReportFile.get('object_url')
                r_core_deliver = core_deliver_report(
                    env_vars,
                    jwt_token,
                    report
                )
                if r_core_deliver.get("success"):
                    print(
                        f"Rapport {report.get('code')} livré avec succès.")
                else:
                    print(
                        f"Erreur lors de la livraison du rapport {report.get('code')}")
                    
            # Remove the temporary file
            os.remove(output_filepath)
    except Exception as e:
        print(f"Erreur lors de la livraison du rapport {report.get('code')}")
        print(e)
        return
