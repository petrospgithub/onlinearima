package points;

public interface PointST {
    public Integer getId();
    public Double getLongitude();
    public Double getLatitude();
    public Double getSpeed();
    public Double getHeading();
    public Long getTimestamp();

    public void setSpeed(Double value);
    public void setHeading(Double value);
}
