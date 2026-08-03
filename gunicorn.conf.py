"""
gunicorn.conf.py

Configuração do Gunicorn. O Gunicorn carrega esse arquivo sozinho, sem
precisar passar --config em lugar nenhum — só precisa estar na raiz do
projeto, com esse nome exato.

O hook `post_fork` abaixo é o que resolve um bug sutil: se a gente
inicia as threads de fundo (monitoramento de queda, backup) direto na
importação do `servidor.py`, elas nascem no processo MESTRE do Gunicorn
— que só administra os processos operários, nunca atende requisição HTTP
nenhuma. Como threads não sobrevivem ao "clone" que cria o processo
operário, a checagem ficava rodando sozinha, olhando pra uma cópia da
memória que nunca recebia nenhum checkin de verdade — parecia que
"nada funcionava", sem erro nenhum aparecer em lugar nenhum.

O `post_fork` roda depois que o processo operário já existe de verdade
— é o lugar certo pra iniciar qualquer coisa que precise "ver" as
requisições que chegam.
"""


def post_fork(server, worker):
    import servidor
    servidor._iniciar_threads_de_fundo()
    server.log.info(f"Threads de fundo iniciadas no processo operário (PID {worker.pid})")
