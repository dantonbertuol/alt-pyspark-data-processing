class Logs:
    '''
    Classe para logs.
    '''

    def __init__(self, log_file: str) -> None:
        '''
        Inicia a classe.

        Args:
            log_file (str): Caminho do arquivo de logs
        '''
        self.log_file = log_file

    def write(self, message: str) -> None:
        '''
        Escreve uma mensagem no arquivo de logs.

        Args:
            message (str): Mensagem a ser escrita
        '''
        with open(self.log_file, mode='a+') as f:
            f.write(message + '\n')
