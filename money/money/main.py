"""
Script para execução standalone da automação de cotações do Banco Central.
Pode ser executado diretamente sem o Dagster.
"""

import os
import sys
import time
import traceback
from datetime import date, datetime
from typing import Optional

# Adiciona o diretório raiz ao PYTHONPATH para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from money.utils.selenium_utils import BCBAutomation
from money.config import setup_logging

def clear_screen():
    """Limpa a tela do terminal"""
    os.system('cls' if os.name == 'nt' else 'clear')


def main():
    """Menu principal"""
    try:
        setup_logging()  # Configura o logging
        
        clear_screen()
        print("\n====== AUTOMAÇÃO DE COTAÇÕES DO BANCO CENTRAL ======")
        print("1. Executar automação completa")
        print("2. Debug do banner de cookies")
        print("3. Criar arquivo de exemplo de cotações")
        print("4. Executar automação com navegador visível")
        print("5. Recriar arquivo de exemplo de cotações (substituir existente)")
        print("0. Sair")

        option = input("\nEscolha uma opção: ")

        if option == "1":
            try:
                start_time = time.time()
                automation = BCBAutomation()
                output_file = automation.run()
                elapsed_time = time.time() - start_time

                print(f"\nAutomação concluída em {elapsed_time:.2f} segundos.")
                print(f"Relatório gerado em: {output_file}")

            except Exception as e:
                print(f"\nErro na execução da automação: {str(e)}")

        elif option == "2":
            try:
                print("\n--- INICIANDO DEBUG DO BANNER DE COOKIES ---")
                # Código simplificado de debug do banner
                automation = BCBAutomation(headless=False, debug_screenshots=True)
                automation.setup_driver()

                try:
                    automation.driver.get("https://www.bcb.gov.br/conversao")
                    time.sleep(5)

                    # Salva screenshot
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    os.makedirs("debug", exist_ok=True)
                    filename = f"debug/initial_page_{timestamp}.png"
                    automation.driver.save_screenshot(filename)
                    print(f"Screenshot inicial salvo: {filename}")

                    # Tenta fechar o banner
                    automation._close_banners()

                    # Aguarda para verificar resultado
                    time.sleep(5)
                    after_filename = f"debug/after_banner_close_{timestamp}.png"
                    automation.driver.save_screenshot(after_filename)
                    print(f"Screenshot após interação com banner: {after_filename}")

                    print("\nDebug do banner de cookies concluído!")

                finally:
                    automation.close_driver()

            except Exception as e:
                print(f"\nErro no debug do banner: {str(e)}")

        elif option == "3":
            try:
                automation = BCBAutomation()
                filepath = automation.create_sample_input_file()
                print(f"\nArquivo de exemplo criado em: {filepath}")
            except Exception as e:
                print(f"\nErro ao criar arquivo de exemplo: {str(e)}")

        elif option == "4":
            try:
                start_time = time.time()
                automation = BCBAutomation(headless=False, debug_screenshots=True)  # Navegador visível
                output_file = automation.run()
                elapsed_time = time.time() - start_time

                print(f"\nAutomação concluída em {elapsed_time:.2f} segundos.")
                print(f"Relatório gerado em: {output_file}")

            except Exception as e:
                print(f"\nErro na execução da automação: {str(e)}")

        elif option == "5":
            try:
                automation = BCBAutomation()
                filepath = automation.create_sample_input_file(force=True)
                print(f"\nArquivo de exemplo recriado em: {filepath}")
            except Exception as e:
                print(f"\nErro ao recriar arquivo de exemplo: {str(e)}")

        elif option == "0":
            print("\nEncerrando programa...")
            return

        else:
            print("\nOpção inválida! Por favor, escolha uma opção válida.")

        # Perguntar se deseja continuar
        input("\nPressione ENTER para continuar...")
        main()  # Chama recursivamente para voltar ao menu

    except KeyboardInterrupt:
        print("\nPrograma interrompido pelo usuário.")
    except Exception as e:
        print(f"\nErro inesperado: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro fatal: {str(e)}")