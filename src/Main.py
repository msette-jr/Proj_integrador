from Etlbcb import *

class Main:
    def __init__(self):
        pass  # Não há necessidade de lógica no construtor por enquanto.

    def iniciarETL(self):
        # URL fornecida para o processo ETL
        url = "https://olinda.bcb.gov.br/opython mainlinda/servico/MPV_DadosAbertos/versao/v1/odata/Quantidadeetransacoesdecartoes(trimestre=@trimestre)?@trimestre='20051'&$top=100000&$format=json"
        etl = etlBcb(url)  # Inicializa a classe etlBcb com a URL
        etl.executar_etl('value', 'transacaoCartao', 'transacaoCartao.csv')  # Executa o método ETL com os parâmetros
        
        
        url2 ="https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoSelic?$top=10000&$format=json"
        etl = etlBcb(url2)
        etl.executar_etl('value', 'expectativaMercadoSelic', 'expectativaMercadoSelic.csv')
        
        url3 ="https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/TransacoesPixPorMunicipio(DataBase=@DataBase)?@DataBase=%27202202%27&$format=json"
        etl = etlBcb(url3)
        etl.executar_etl('value', 'TransacoesPixPorMunicipio', 'TransacoesPixPorMunicipio.csv')
# Instancia a classe Main e executa o método iniciarETL
if __name__ == "__main__":
    main = Main()
    main.iniciarETL()
