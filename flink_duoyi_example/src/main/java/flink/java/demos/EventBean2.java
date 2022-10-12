package flink.java.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventBean2 {
    private long guid;
    private String evrntId;
    private long timeStamp;
    private String pageId;
    private int actTimelong;
}
