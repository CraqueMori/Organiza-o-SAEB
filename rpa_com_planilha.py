from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import openpyxl
import time
import pandas as pd
import numpy as np

konecta = pd.read_excel('KONECTA SAEB.xslx', sheet_name=0, engine='openpyxl')




# Função para fazer login
def login(driver, username, password):
    driver.get('https://seduc.am.plural.plus/admin/planos')
    username_field = driver.find_element(By.ID, 'email')
    password_field = driver.find_element(By.ID, 'senha')
    time.sleep(2)
    username_field.send_keys(username)
    password_field.send_keys(password)
    login_button = driver.find_element(By.ID, 'bt_login')
    login_button.click()
    time.sleep(5)

# Função para procurar a escola usando a barra de pesquisa e clicar no ícone
def search_school_and_click_icon(driver, school_name):
    try:
        search_box = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="pchave"]'))
        )
        search_box.send_keys(school_name)
        search_box.send_keys(Keys.RETURN)
        time.sleep(5)  # Esperar os resultados da pesquisa carregar
        
        school_row = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, f'//td[contains(text(), "{school_name}")]/ancestor::tr'))
        )
        info_icon = school_row.find_element(By.XPATH, './/td[4]/i')
        info_icon.click()
        time.sleep(2)  # Esperar o pop-up aparecer
        return True
    except Exception as e:
        print(f"Erro ao encontrar a escola {school_name} e clicar no ícone: {e}")
        return False

# Função para encontrar e clicar no hyperlink
def click_hyperlink(driver, keyword):
    try:
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="body_cursos_plano"]'))
        )
        articles = driver.find_elements(By.XPATH, '//*[@id="body_cursos_plano"]/a')
        for article in articles:
            if keyword in article.text:
                article.click()
                return True
        print(f"Nenhum hyperlink encontrado com {keyword}")
        return False
    except Exception as e:
        print(f"Erro ao clicar no hyperlink: {e}")
        return False

# Função para editar e salvar informações
def edit_and_save_info(driver, professor_name):
    try:
        # Clicar no botão de editar
        edit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div/div[2]/div[1]/div/ul/li[10]/a'))
        )
        edit_button.click()
        time.sleep(5)  # Esperar a página de edição carregar
        
        # Copiar o valor da div especificada
        info_element = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="form_dados"]/div/div[13]/div/div/span/span[1]/span/ul/li[1]'))
        )
        info_text = info_element.text
        
        # Clicar na caixa de professor e digitar o nome do professor
        professor_box = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="form_dados"]/div/div[14]/div/div/span/span[1]/span/ul/li/input'))
        )
        professor_box.click()
        professor_box.send_keys(professor_name)
        professor_box.send_keys(Keys.RETURN)
        time.sleep(2)
        
        # Clicar no botão de submit para salvar
        submit_button = driver.find_element(By.XPATH, '//*[@id="bt-dados"]')
        submit_button.click()
        time.sleep(5)  # Esperar a submissão
        
        return info_text
    except Exception as e:
        print(f"Erro ao editar e salvar informações: {e}")
        return None

# Configurar o webdriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Fazer login
username = 'luis.henrique@innyx.com'
password = '123456'
login(driver, username, password)

# Acessar a URL após o login
driver.get('https://seduc.am.plural.plus/admin/planos')
time.sleep(5)  # Esperar a página carregar

# Nome da escola a ser procurada
school_name = "COORD. DISTRITAL 01_COLEGIO BRASILEIRO PEDRO SILVESTRE_1 SERIE_1"

# Procurar a escola e clicar no ícone
if search_school_and_click_icon(driver, school_name):
    # Palavra-chave para o hyperlink
    keyword = "GEOGRAFIA"

    # Clicar no hyperlink correspondente
    if click_hyperlink(driver, keyword):
        # Nome do professor a ser digitado
        professor_name = "LUIZ FERNANDO DE MORAES LOBATO"
        
        # Editar e salvar informações
        info = edit_and_save_info(driver, professor_name)
        
        if info:
            # Abrir a planilha para salvar os dados
            workbook = openpyxl.Workbook()
            sheet = workbook.active
            sheet['A1'] = 'Informação'
            sheet['A2'] = info
            
            # Salvar a planilha
            workbook.save('informacoes.xlsx')

# Fechar o navegador
driver.quit()
