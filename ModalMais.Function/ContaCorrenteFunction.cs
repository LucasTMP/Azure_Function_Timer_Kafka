using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ModalMais.Function
{
    public static class ContaCorrenteFunction
    {
        [FunctionName("ContaCorrenteFunction")]
        public static async Task Run([TimerTrigger("* * * * *")] TimerInfo myTimer, ILogger log)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                //BootstrapServers = Environment.GetEnvironmentVariable("Kafka"),
                GroupId = "ConsumerConta",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var cancelToken = new CancellationTokenSource().Token;
            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                var quantidadeMensagens = 0;

                try
                {
                    log.LogInformation($"Capturando mensagens do kafka");
                    while (true)
                    {
                        consumer.Subscribe("CADASTRO_CONTA_CORRENTE_ATUALIZADO");
                        var consumeResult = consumer.Consume(cancelToken);
                        await ArmazenarExtratoCacheado(consumeResult.Message.Key.ToString(), consumeResult.Message.Value);
                        log.LogInformation($"Mensagens capturadas: {++quantidadeMensagens}");
                    }
                }
                catch
                {
                    log.LogInformation($"Todas mensagens foram lidas.");
                    consumer.Close();
                }

            }
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }


        public static async Task ArmazenarExtratoCacheado(string key, string contaId)
        {
            var dataCriacao = DateTime.Now;
            var chave = GerarChaveRedis(contaId, dataCriacao, dataCriacao.AddHours(24));

            var horas = dataCriacao.Hour;
            var minutos = dataCriacao.Minute;
            var segundos = dataCriacao.Second;
            var Expiracao = new TimeSpan(24, 0, 0) - new TimeSpan(horas, minutos, segundos);

            var connString = Environment.GetEnvironmentVariable("ConnectionStrings:Redis");
            //var connString = Environment.GetEnvironmentVariable("Redis");

            var redis = ConnectionMultiplexer.Connect(connString).GetDatabase();
            await redis.StringSetAsync(chave, $"{key}:{contaId}", Expiracao);
        }

        public static string GerarChaveRedis(string contaId, DateTime dataInicio, DateTime dataFinal)
        {
            return $"Conta:{contaId}-DataCriacao:{dataInicio:dd-MM-yyyy}-DataExpiracao:{dataFinal:dd-MM-yyyy}";
        }

    }
}
