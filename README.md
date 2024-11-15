# Resumo
Projeto pessoal de testes para estudos sobre o framework 'kafka-streams'.

## Conteúdo

### LineSplit
- Configura a conexão com o servidor Kafka;
- Conecta-se ao tópico de entrada "streams-plaintext-input";
- A medida em que recebe as mensagens do tópico, separa cada palavra;
- Posta cada uma dessas palavras separadamente (cada uma sendo uma nova mensagem) no tópico de saída "streams-linesplit-output";
- Cria um desenho desta topologia e exibe na console;

### Pipe
- Configura a conexão com o servidor Kafka;
- Conecta-se ao tópico de entrada "streams-plaintext-input";
- Conecta-se ao tópico de saída "streams-pipe-output";
- Cria um desenho desta topologia e exibe na console;

### WordCount
- Configura a conexão com o servidor Kafka;
- Conecta-se ao tópico de entrada "streams-plaintext-input";
- A medida em que recebe as mensagens do tópico, separa cada palavra;
- Posta cada uma dessas palavras separadamente (cada uma sendo uma nova mensagem) no tópico de saída "streams-linesplit-output";
- Conta a quantidade de palavras quebradas e armazena em um storage no Kafka chamado de "counts-store";
- Posta o resultado desta contagem em um tópico chamado "streams-wordcount-output";
- Cria um desenho desta topologia e exibe na console;