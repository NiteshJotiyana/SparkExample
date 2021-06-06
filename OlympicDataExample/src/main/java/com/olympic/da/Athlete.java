package com.olympic.da;

public class Athlete {

	String name;
	String country;
	int totalMedal;
	
	
	public Athlete(String name, String country, int totalMedal) {
		super();
		this.name = name;
		this.country = country;
		this.totalMedal = totalMedal;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public int getTotalMedal() {
		return totalMedal;
	}
	public void setTotalMedal(int totalMedal) {
		this.totalMedal = totalMedal;
	}
	
	
}
