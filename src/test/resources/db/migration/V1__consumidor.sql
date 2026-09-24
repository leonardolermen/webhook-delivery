-- Migration do "consumidor" do contexto de teste, no schema default (public). Versão 1 de propósito:
-- é a mesma versão da V1 da lib, e as duas só convivem porque os históricos são separados.
CREATE TABLE consumidor_marcador (id INT);
