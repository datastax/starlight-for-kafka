/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.DataOutputStreamWritable;
import org.apache.kafka.common.utils.Utils;

/**
 * This class is necessary to bypass a bug that is fixed here https://github.com/apache/kafka/pull/13032. When
 * that PR is available upstream, we can remove this class.
 */
public class KopDataOutputStreamWritable extends DataOutputStreamWritable {
    public KopDataOutputStreamWritable(DataOutputStream out) {
        super(out);
    }
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        try {
            if (buf.hasArray()) {
                out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            } else {
                byte[] bytes = Utils.toArray(buf);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
