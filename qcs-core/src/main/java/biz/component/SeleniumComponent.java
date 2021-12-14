package biz.component;

import cn.hutool.core.thread.ThreadUtil;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;
import lombok.extern.slf4j.Slf4j;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Component
@Slf4j
public class SeleniumComponent {

    @Value("${selenium.driver.endpoint}")
    private String driverEndpoint;

    @Value("${report.htmlToPdf.dealDataSleepTime}")
    private int sleepTime;

    @Value("${qcs.custom.casAccount}")
    private String casAccount;

    @Value("${qcs.custom.casPassword}")
    private String casPassword;

    public String getPageSource(String url) {
        RemoteWebDriver browser = null;
        try {
            browser = getBrowser();
            browser.get(url);
            return browser.getPageSource();
        } finally {
            if (browser != null) {
                browser.close();
            }
        }
    }

    public byte[] screenshotAsImageBytes(String src, ExpectedCondition<Boolean> condition) {
        return screenshotAs(src, OutputType.BYTES, condition);
    }

    public File screenshotAsImage(String src) {
        return screenshotAsImage(src, null);
    }

    public File screenshotAsImage(String src, ExpectedCondition<Boolean> condition) {
        return screenshotAs(src, OutputType.FILE, condition);
    }

    public void screenshotAsPDF(String src, String dst, ExpectedCondition<Boolean> condition) {
        Document document = null;
        try {
            Image image = Image.getInstance(screenshotAsImageBytes(src, condition));
            float height = image.getHeight();
            float width = image.getWidth();
            image.setAlignment(Image.MIDDLE);

            document = new Document(new Rectangle(width, height));
            PdfWriter.getInstance(document, new FileOutputStream(dst));
            document.open();
            document.add(image);
        } catch (DocumentException | IOException e) {
            throw new RuntimeException("gen page pdf failed.", e);
        } finally {
            if (document != null) {
                document.close();
            }
        }
    }

    private void imageToPDF(Image image, String dst) {
        Document document = null;
        try {
            float height = image.getHeight();
            float width = image.getWidth();
            image.setAlignment(Image.MIDDLE);

            document = new Document(new Rectangle(width, height));
            PdfWriter.getInstance(document, new FileOutputStream(dst));
            document.open();
            document.add(image);
        } catch (DocumentException | IOException e) {
            throw new RuntimeException("gen page pdf failed.", e);
        } finally {
            if (document != null) {
                document.close();
            }
        }
    }

    private <T> T screenshotAs(String url, OutputType<T> outputType, ExpectedCondition<Boolean> condition) {
        RemoteWebDriver browser = null;
        try {
            browser = getBrowser();
            browser.get(url);

            if (condition != null) {
                WebDriverWait wait = new WebDriverWait(browser, 10);
                wait.until(condition);
            }
            ThreadUtil.sleep(1000);
            try{
                // 统一鉴权登录界面
                List<WebElement> webElementList = browser.findElementsByClassName("required");
                webElementList.get(0).sendKeys(casAccount);
                webElementList.get(1).sendKeys(casPassword);
                browser.findElementByClassName("loginBut").click();
                ThreadUtil.sleep(1000);
            }catch (Exception e){
                log.error("未设置为统一鉴权登录流程", e);
            }finally {
                browser.get(url);
                ThreadUtil.sleep(sleepTime);
            }

            // 设置窗口宽高
            String height = browser.executeScript("return document.documentElement.scrollHeight").toString();
            browser.manage().window().setSize(new Dimension(1920, Integer.parseInt(height)));
            return browser.getScreenshotAs(outputType);
        } finally {
            if (browser != null) {
                browser.close();
            }
        }
    }

    private RemoteWebDriver getBrowser() {
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless");
        options.addArguments("--disable-gpu");
        options.addArguments("--no-sandbox");
        options.addArguments("--privileged");
        options.addArguments("-–single-process");
        DesiredCapabilities desiredCapabilities = DesiredCapabilities.chrome();
        desiredCapabilities.setCapability(ChromeOptions.CAPABILITY, options);
        try {
            return new RemoteWebDriver(new URL(driverEndpoint), desiredCapabilities);
        } catch (MalformedURLException e) {
            throw new RuntimeException("init WebDriver failed.", e);
        }
    }
}
