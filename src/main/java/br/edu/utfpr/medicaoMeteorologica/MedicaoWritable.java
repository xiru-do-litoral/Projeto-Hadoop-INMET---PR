package br.edu.utfpr.medicaoMeteorologica;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class MedicaoWritable implements WritableComparable<MedicaoWritable> {

	private String ano;
	private String estacao;

	public MedicaoWritable() {

	}

	public MedicaoWritable(String ano, String estacao) {
		this.ano = ano;
		this.estacao = estacao;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, ano);
		Text.writeString(out, estacao);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ano = Text.readString(in);
		estacao = Text.readString(in);
	}

	public String getAno() {
		return this.ano;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(estacao).append(ano).toHashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MedicaoWritable)) {
			return false;
		}

		final MedicaoWritable other = (MedicaoWritable) o;
		return new EqualsBuilder().append(estacao, other.estacao).append(ano, other.ano).isEquals();
	}

	@Override
	public String toString() {
		return "(" + ano + ") - " +  (estacao+"                    ").substring(0,14);
	}

	@Override
	public int compareTo(MedicaoWritable medicaoWritable) {
		return new CompareToBuilder().append(this, medicaoWritable).toComparison();
	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(MedicaoWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(MedicaoWritable.class, new Comparator());
	}

}
