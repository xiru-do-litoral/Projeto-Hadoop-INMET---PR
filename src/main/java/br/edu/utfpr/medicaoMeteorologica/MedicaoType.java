package br.edu.utfpr.medicaoMeteorologica;

public enum MedicaoType {

	PRECIPITACAO("precipitacao", 3),
	TEMP_MAXIMA("temp_maxima", 4),
	TEMP_MINIMA("temp_minima", 5),
	INSOLACAO("insolacao", 6),
	EVAPORACAO_PICHE("evaporacao_piche", 7),
	TEMP_COMP("temp_comp", 8),
	UMIDADE_RELATIVA("umidade_relativa", 9),
	VELOCIDADE_VENTO("velocidade_vento", 10);
	
	private final String type;
	private final int order;

	private MedicaoType(String value, int order) {
		this.type = value;
		this.order = order;
	}

	public String getType() {
		return type;
	}

	public static int getOrder(String type) {

		for (MedicaoType medicaoType : MedicaoType.values()) {
			if (medicaoType.getType().equals(type)) {
				return medicaoType.order;
			}
		}

		// Valor padrão caso não seja corretamente declarada a variável (coluna) a procurar
		return MedicaoType.PRECIPITACAO.order;
	}
}
