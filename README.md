## 📋 Tabla Resumen de Comandos

| Comando                           | Uso                                                                       |
| --------------------------------- | ------------------------------------------------------------------------- |
| `git init`                        | Inicializa un nuevo repositorio Git local.                                |
| `git status`                      | Muestra el estado de los archivos (cambios, pendientes, sin seguimiento). |
| `git add .`                       | Añade todos los cambios al área de preparación (staging).                 |
| `git commit -m "mensaje"`         | Guarda los cambios en el historial local con un mensaje.                  |
| `git remote add origin <URL>`     | Conecta el repositorio local con uno remoto en GitHub.                    |
| `git remote -v`                   | Lista las URLs de los repositorios remotos configurados.                  |
| `git remote set-url origin <URL>` | Cambia la URL del repositorio remoto.                                     |
| `git push -u origin main`         | Sube los cambios locales a la rama `main` del remoto.                     |
| `git push`                        | Envía los commits locales al remoto (ya configurado).                     |
| `git pull origin main --rebase`   | Actualiza la rama local trayendo cambios del remoto sin perder commits.   |
| `git push origin main --force` ⚠️ | Fuerza la subida, sobrescribiendo lo que hay en remoto.                   |

